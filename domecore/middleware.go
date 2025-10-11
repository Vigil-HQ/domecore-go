package domecore

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"connectrpc.com/grpchealth"
	"github.com/rs/cors"
	"go.uber.org/zap"
)

type grpcAuthMiddleware struct {
	loggR         *zap.Logger
	authenticator Authenticator
	contextHelper ContextHelper
}

func (middleware *grpcAuthMiddleware) UnaryTenantInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			tenantID := req.Header().Get(XTenantKey)
			if tenantID == "" {
				return nil, ErrMissingTenantHeader
			}

			newCtx := context.WithValue(ctx, XTenantKey, tenantID)
			return next(newCtx, req)
		}
	}
}

func (middleware *grpcAuthMiddleware) UnaryTokenInterceptor(routes ...string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			fullMethod := req.Spec().Procedure
			if slices.Contains(routes, fullMethod) {
				return next(ctx, req)
			}

			token, err := middleware.authenticator.ExtractHeaderToken(req)
			if err != nil {
				return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing or invalid token: %v", err))
			}

			idToken, err := middleware.authenticator.GetVerifier().Verify(ctx, token)
			if err != nil {
				return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("invalid token: %v", err))
			}

			claims := new(UserAuthClaims)
			if err := idToken.Claims(claims); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to parse token claims: %v", err))
			}

			newCtx := context.WithValue(ctx, ContextKeyUser, claims)
			return next(newCtx, req)
		}
	}
}

// LoggingUnaryInterceptor logs sanitized gRPC request and response data
func (middleware *grpcAuthMiddleware) LoggingUnaryInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			fullMethod := request.Spec().Procedure

			sanitizedReq := middleware.sanitizeRequest(request)

			middleware.loggR.Info("gRPC request received",
				zap.String("method", fullMethod),
				zap.Any("request", sanitizedReq),
			)

			resp, err := next(ctx, request)
			duration := time.Since(start)

			if err != nil {
				middleware.loggR.Error("gRPC request failed",
					zap.String("method", fullMethod),
					zap.Error(err),
					zap.Duration("duration", duration),
				)
			} else {
				middleware.loggR.Info("gRPC request completed",
					zap.String("method", fullMethod),
					zap.Any("response", resp),
					zap.Duration("duration", duration),
				)
			}

			return resp, err
		}
	}
}

// CorsMiddleware sets CORS configuration for HTTP server
func (middleware *grpcAuthMiddleware) CorsMiddleware(h http.Handler) http.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins:       []string{"*"},
		AllowedMethods:       connectcors.AllowedMethods(),
		AllowedHeaders:       []string{"*"},
		ExposedHeaders:       connectcors.ExposedHeaders(),
		AllowCredentials:     false,
		OptionsSuccessStatus: 200,
	})
	return c.Handler(h)
}

// HealthChecker returns a static gRPC health checker
func (middleware *grpcAuthMiddleware) HealthChecker(srvName string) *grpchealth.StaticChecker {
	return grpchealth.NewStaticChecker(srvName)
}

// sanitizeRequest masks sensitive fields in request struct
func (middleware *grpcAuthMiddleware) sanitizeRequest(req interface{}) interface{} {
	sensitiveFields := map[string]struct{}{
		"password": {},
		"token":    {},
		"secret":   {},
		"apikey":   {},
		"apiKey":   {},
		"auth":     {},
	}
	return sanitize(req, sensitiveFields)
}

func sanitize(v interface{}, sensitiveFields map[string]struct{}) interface{} {
	if v == nil {
		return nil
	}

	rv := reflect.ValueOf(v)
	rt := reflect.TypeOf(v)

	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		rv = rv.Elem()
		rt = rt.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return v
	}

	copied := reflect.New(rt).Elem()
	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)
		value := rv.Field(i)
		fieldName := strings.ToLower(field.Name)

		if !value.CanInterface() {
			continue
		}

		if _, isSensitive := sensitiveFields[fieldName]; isSensitive {
			if field.Type.Kind() == reflect.String {
				copied.Field(i).SetString("[REDACTED]")
			} else {
				copied.Field(i).Set(reflect.Zero(field.Type))
			}
		} else if field.Type.Kind() == reflect.Struct {
			sanitized := sanitize(value.Interface(), sensitiveFields)
			copied.Field(i).Set(reflect.ValueOf(sanitized))
		} else {
			copied.Field(i).Set(value)
		}
	}
	return copied.Addr().Interface()
}

// NewMiddleware  returns a new instance of grpcAuthMiddleware
func NewMiddleware(authenticator Authenticator, logger *zap.Logger, contextHelper ContextHelper) Middleware {
	return &grpcAuthMiddleware{
		loggR:         logger,
		authenticator: authenticator,
		contextHelper: contextHelper,
	}
}
