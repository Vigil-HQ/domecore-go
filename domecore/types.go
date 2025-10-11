package domecore

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"github.com/coreos/go-oidc"
	"github.com/golang-jwt/jwt/v4"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

type PagedResult[T any] struct {
	Items T
	Total int64
}

type UserAuthClaims struct {
	Exp               int64          `json:"exp"`
	Iat               int64          `json:"iat"`
	Jti               string         `json:"jti"`
	Iss               string         `json:"iss"`
	Aud               []string       `json:"aud"`
	Id                string         `json:"sub"`
	Typ               string         `json:"typ"`
	Azp               string         `json:"azp"`
	Sid               string         `json:"sid"`
	Acr               string         `json:"acr"`
	AllowedOrigins    []string       `json:"allowed-origins"`
	RealmAccess       RealmAccess    `json:"realm_access"`
	ResourceAccess    ResourceAccess `json:"resource_access"`
	Scope             string         `json:"scope"`
	EmailVerified     bool           `json:"email_verified"`
	Organization      []string       `json:"organization"`
	Name              string         `json:"name"`
	PreferredUsername string         `json:"preferred_username"`
	GivenName         string         `json:"given_name"`
	FamilyName        string         `json:"family_name"`
	Email             string         `json:"email"`
	jwt.RegisteredClaims
}

// RealmAccess defines roles at the realm level
type RealmAccess struct {
	Roles []string `json:"roles"`
}

// ResourceAccess defines roles at the resource level
type ResourceAccess struct {
	Account AccountRoles `json:"account"`
}

// AccountRoles defines roles within the "account" resource
type AccountRoles struct {
	Roles []string `json:"roles"`
}

type Config interface {
	LoadEnv()
	GetGormConfig() *gorm.Config
	Logger() *zap.Logger
	Http2() *http2.Server
	JetStream() jetstream.StreamConfig
	GetServerAddr() string
	GetEnvironment() string
	IsTesting() bool
	IsDevelopment() bool
	IsProduction() bool
}

type ContextHelper interface {
	GetTenant(context.Context) (string, error)
	GetUserClaims(context.Context) *UserAuthClaims
	GetAccessToken(request connect.AnyRequest) (string, error)
}

type Authenticator interface {
	ExtractHeaderToken(connect.AnyRequest) (string, error)
	ExtractToken(ctx context.Context) (string, error)
	GetVerifier() *oidc.IDTokenVerifier
	ValidateTokenMiddleware(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
}

// Middleware types
type Middleware interface {
	CorsMiddleware(http.Handler) http.Handler
	LoggingUnaryInterceptor() connect.UnaryInterceptorFunc
	HealthChecker(string) *grpchealth.StaticChecker
	UnaryTokenInterceptor(...string) connect.UnaryInterceptorFunc
	UnaryTenantInterceptor() connect.UnaryInterceptorFunc
}
