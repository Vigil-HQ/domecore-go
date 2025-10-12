package domecore

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/coreos/go-oidc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type keycloakAuthenticator struct {
	verifier *oidc.IDTokenVerifier
}

func (authenticator *keycloakAuthenticator) ExtractHeaderToken(request connect.AnyRequest) (string, error) {
	// Look for the authorization header.
	authHeader := request.Header().Get("Authorization")
	if authHeader == "" {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	// The authorization header should be in the form "Bearer <token>".
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", status.Error(codes.Unauthenticated, "invalid authorization header")
	}

	return parts[1], nil
}

func (authenticator *keycloakAuthenticator) GetVerifier() *oidc.IDTokenVerifier {
	return authenticator.verifier
}

func NewAuthenticator(ctx context.Context) (Authenticator, error) {
	clientId := os.Getenv("KC.CLIENT_ID")
	issuerUrl := os.Getenv("KC.BASE_URL")
	url := fmt.Sprintf("%s/realms/%s", issuerUrl, os.Getenv("KC.REALM"))

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	}

	client := &http.Client{
		Timeout:   time.Duration(2) * time.Minute,
		Transport: tr,
	}

	c := oidc.ClientContext(ctx, client)
	provider, err := oidc.NewProvider(c, url)
	if err != nil {
		return nil, err
	}

	oidcConfig := &oidc.Config{
		ClientID: clientId,
	}

	verifier := provider.Verifier(oidcConfig)

	return &keycloakAuthenticator{
		verifier: verifier,
	}, nil
}

// ExtractToken extracts the bearer token from the gRPC metadata (authorization header).
func (authenticator *keycloakAuthenticator) ExtractToken(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing metadata")
	}

	// Look for the authorization header.
	authHeader, ok := md["authorization"]
	if !ok || len(authHeader) == 0 {
		return "", status.Error(codes.Unauthenticated, "missing authorization header")
	}

	// The authorization header should be in the form "Bearer <token>".
	parts := strings.SplitN(authHeader[0], " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", status.Error(codes.Unauthenticated, "invalid authorization header")
	}

	return parts[1], nil
}

// ValidateTokenMiddleware validates the JWT token in the authorization header.
func (authenticator *keycloakAuthenticator) ValidateTokenMiddleware(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	// Extract and validate the token from metadata (authorization header).
	token, err := authenticator.ExtractToken(ctx)
	if err != nil {
		return nil, err
	}

	// Parse and verify the token.
	idToken, err := authenticator.GetVerifier().Verify(ctx, token)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("failed to verify token: %v", err))
	}

	// Get the claims from the token.
	claims := new(UserAuthClaims)
	if err := idToken.Claims(claims); err != nil {
		return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("failed to verify claims: %v", err))
	}

	// Pass the claims into the context for further use in the handler.
	ctx = context.WithValue(ctx, ContextKeyUser, claims)

	return handler(ctx, req)
}
