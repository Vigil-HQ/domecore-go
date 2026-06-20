// Package domecore provides authentication and authorization functionality for DOME services,
// including OIDC token validation and middleware for gRPC and Connect-RPC services.
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

// keycloakAuthenticator implements the Authenticator interface for Keycloak OIDC authentication.
// It wraps an OIDC ID token verifier and provides methods to extract and validate JWT tokens
// from HTTP headers and gRPC metadata.
type keycloakAuthenticator struct {
	verifier *oidc.IDTokenVerifier
}

// ExtractHeaderToken extracts the bearer token from a Connect-RPC request's Authorization header.
// It validates that the header is present and properly formatted as "Bearer <token>".
// Returns the token string or an error if the header is missing or malformed.
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

// GetVerifier returns the underlying OIDC ID token verifier used for validating JWT tokens.
// This verifier is configured with the client ID and issuer information during authenticator initialization.
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
// It retrieves the metadata from the incoming context, locates the authorization header,
// and validates that it follows the "Bearer <token>" format.
// Returns the token string or an error if metadata is missing, the header is absent, or the format is invalid.
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

// ValidateTokenMiddleware is a gRPC unary interceptor that validates JWT tokens in incoming requests.
// It extracts the token from the authorization header, verifies its signature and validity,
// parses the claims into a UserAuthClaims struct, and adds the claims to the request context
// for use by downstream handlers. Returns an authentication error if any validation step fails.
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
