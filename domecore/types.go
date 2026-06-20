// Package domecore provides core types, interfaces, and utilities for the RIM (Resource Identity Management) system.
// It defines authentication structures, configuration interfaces, middleware components, and common data types
// used across the application for handling JWT claims, OIDC authentication, database operations, and HTTP/gRPC services.
package domecore

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/otelconnect"
	"github.com/beego/beego/v2/core/logs"
	"github.com/coreos/go-oidc"
	"github.com/golang-jwt/jwt/v4"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

// PagedResult is a generic struct that represents a paginated result set.
// It contains a collection of items of any type T and the total count of items available.
type PagedResult[T any] struct {
	// Items hold the actual data items for the current page
	Items T
	// Total represents the total number of items across all pages
	Total int64
}

// UserAuthClaims represents the JWT claims structure for authenticated users.
// It extends the standard JWT claims with Keycloak-specific fields including user identity,
// roles, organization membership, and profile information.
type UserAuthClaims struct {
	// Exp is the expiration time (Unix timestamp) after which the token is invalid
	Exp int64 `json:"exp"`
	// Iat is the issued at the time (Unix timestamp) when the token was created
	Iat int64 `json:"iat"`
	// Jti is the JWT ID, a unique identifier for this token
	Jti string `json:"jti"`
	// Iss is the issuer, typically the Keycloak server URL
	Iss string `json:"iss"`
	// Aud is the audience, the intended recipients of this token
	Aud []string `json:"aud"`
	// ID is the subject identifier, typically the user's unique ID
	Id string `json:"sub"`
	// Typ is the token type, usually "Bearer"
	Typ string `json:"typ"`
	// Azp is the authorized party, the client ID that requested this token
	Azp string `json:"azp"`
	// Sid is the session ID for this authentication session
	Sid string `json:"sid"`
	// Acr is the authentication context class reference
	Acr string `json:"acr"`
	// AllowedOrigins contains the list of origins allowed to use this token
	AllowedOrigins []string `json:"allowed-origins"`
	// RealmAccess contains roles granted at the realm level
	RealmAccess RealmAccess `json:"realm_access"`
	// ResourceAccess contains roles granted at the resource/client level
	ResourceAccess ResourceAccess `json:"resource_access"`
	// Scope contains the OAuth2 scopes granted to this token
	Scope string `json:"scope"`
	// EmailVerified indicates whether the user's email has been verified
	EmailVerified bool `json:"email_verified"`
	// Organization contains the organizations the user belongs to.
	// Keycloak can emit this claim as either an array or an object keyed by alias.
	Organization OrganizationsClaim `json:"organization"`
	// Name is the user's full name
	Name string `json:"name"`
	// PreferredUsername is the user's preferred username for display
	PreferredUsername string `json:"preferred_username"`
	// GivenName is the user's first name
	GivenName string `json:"given_name"`
	// FamilyName is the user's last name
	FamilyName string `json:"family_name"`
	// Email is the user's email address
	Email string `json:"email"`
	// RegisteredClaims embeds standard JWT claims
	jwt.RegisteredClaims
}

type OrganizationClaim struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Alias string `json:"alias"`
}

type OrganizationsClaim struct {
	Items []OrganizationClaim
}

func (claim *OrganizationsClaim) UnmarshalJSON(data []byte) error {
	if string(data) == "null" || len(data) == 0 {
		claim.Items = nil
		return nil
	}

	var aliases []string
	if err := json.Unmarshal(data, &aliases); err == nil {
		items := make([]OrganizationClaim, 0, len(aliases))
		for _, alias := range aliases {
			items = append(items, OrganizationClaim{Alias: alias})
		}
		claim.Items = items
		return nil
	}

	type rawOrganization struct {
		ID    string `json:"id"`
		Name  string `json:"name"`
		Alias string `json:"alias"`
	}
	rawMap := map[string]rawOrganization{}
	if err := json.Unmarshal(data, &rawMap); err != nil {
		return err
	}

	keys := make([]string, 0, len(rawMap))
	for key := range rawMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	items := make([]OrganizationClaim, 0, len(keys))
	for _, key := range keys {
		item := rawMap[key]
		if item.Alias == "" {
			item.Alias = key
		}
		items = append(items, OrganizationClaim{
			ID:    item.ID,
			Name:  item.Name,
			Alias: item.Alias,
		})
	}
	claim.Items = items
	return nil
}

func (claim OrganizationsClaim) Aliases() []string {
	aliases := make([]string, 0, len(claim.Items))
	for _, item := range claim.Items {
		if item.Alias != "" {
			aliases = append(aliases, item.Alias)
			continue
		}
		if item.ID != "" {
			aliases = append(aliases, item.ID)
		}
	}
	return aliases
}

func (u UserAuthClaims) GetRole() string {
	// Filter out all keycloak default and service roles
	roles := make([]string, 0)
	for _, role := range u.RealmAccess.Roles {
		// Skip Keycloak default roles
		if role == "default-roles-master" || role == "offline_access" || role == "uma_authorization" {
			continue
		}
		// Skip service account roles
		if len(role) > 8 && role[:8] == "service-" {
			continue
		}
		roles = append(roles, role)
	}

	if len(roles) > 0 {
		return roles[0]
	}
	return ""

}

// RealmAccess defines roles at the realm level.
// These roles are granted globally across the entire Keycloak realm.
type RealmAccess struct {
	// Roles contain the list of role names assigned to the user at the realm level
	Roles []string `json:"roles"`
}

// ResourceAccess defines roles at the resource level.
// It contains roles specific to individual clients/resources within Keycloak.
type ResourceAccess struct {
	// Account contains roles specific to the account management client
	Account AccountRoles `json:"account"`
}

// AccountRoles defines roles within the "account" resource.
// These roles control access to account management features.
type AccountRoles struct {
	// Roles contain the list of role names assigned for account management
	Roles []string `json:"roles"`
}

// Config defines the interface for application configuration management.
// It provides access to environment settings, database configuration, logging,
// HTTP/2 server setup, and environment detection methods.
type Config interface {
	// LoadEnv loads environment variables and initializes configuration
	LoadEnv()
	// GetGormConfig returns the GORM database configuration
	GetGormConfig() *gorm.Config
	// Logger returns the configured Beego logger instance
	Logger() *logs.BeeLogger
	// Http2 returns the HTTP/2 server configuration
	Http2() *http2.Server
	// JetStream returns the NATS JetStream configuration
	JetStream() jetstream.StreamConfig
	// GetServerAddr returns the server address to bind to
	GetServerAddr() string
	// GetEnvironment returns the current environment name (e.g., "development", "production")
	GetEnvironment() string
	// IsTesting returns true if the application is running in testing mode
	IsTesting() bool
	// IsDevelopment returns true if the application is running in development mode
	IsDevelopment() bool
	// IsProduction returns true if the application is running in production mode
	IsProduction() bool
}

// ContextHelper defines the interface for extracting request context information.
// It provides methods to retrieve tenant identifiers, user claims, and access tokens
// from the request context in a multi-tenant application.
type ContextHelper interface {
	// GetTenant extracts the tenant identifier from the given context
	GetTenant(context.Context) (string, error)
	// GetOrganizations extracts all organization aliases from the authenticated user's token claims
	GetOrganizations(context.Context) ([]string, error)
	// GetUserClaims extracts and returns the authenticated user's JWT claims from the context
	GetUserClaims(context.Context) *UserAuthClaims
	// GetAccessToken extracts the access token from the Connect RPC request
	GetAccessToken(request connect.AnyRequest) (string, error)
	// GetTraceID extracts the trace ID from the given context for distributed tracing and request correlation
	GetTraceID(ctx context.Context) string
	// GetRequestID extracts the request ID from the given context for tracking and correlating individual requests
	GetRequestID(ctx context.Context) string
}

// Authenticator defines the interface for authentication and token validation.
// It provides methods to extract, verify, and validate JWT tokens using OIDC (OpenID Connect)
// for both Connect RPC and gRPC services.
type Authenticator interface {
	// ExtractHeaderToken extracts the bearer token from the request headers
	ExtractHeaderToken(connect.AnyRequest) (string, error)
	// ExtractToken extracts the token from the given context
	ExtractToken(ctx context.Context) (string, error)
	// GetVerifier returns the OIDC ID token verifier for validating tokens
	GetVerifier() *oidc.IDTokenVerifier
	// ValidateTokenMiddleware is a gRPC unary interceptor that validates JWT tokens before processing requests
	ValidateTokenMiddleware(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
}

// ResourceResolver defines the interface for resolving procedure names to authorization resources and actions.
// It maps Connect RPC procedure names to their corresponding resource and action identifiers
// used in the authorization policy enforcement.
type ResourceResolver interface {
	// Resolve takes a procedure name and returns the associated resource and action strings
	// used for authorization checks. Returns (resource, action) tuple.
	Resolve(procedure string) (string, string, string, error)
}

// Middleware defines the interface for HTTP and RPC middleware components.
// It provides methods for CORS handling, request logging, health checking,
// token validation, and tenant context extraction for both HTTP and Connect RPC services.
type Middleware interface {
	// Cors CorsMiddleware wraps an HTTP handler with CORS (Cross-Origin Resource Sharing) support
	Cors(http.Handler) http.Handler
	// UnaryLoggingInterceptor returns a Connect RPC interceptor that logs unary requests and responses
	UnaryLoggingInterceptor() connect.UnaryInterceptorFunc
	// HealthChecker creates a static health checker for gRPC health checking protocol with the given service name
	HealthChecker(string) *grpchealth.StaticChecker
	// UnaryTokenInterceptor returns a Connect RPC interceptor that validates tokens, optionally excluding specified procedures
	UnaryTokenInterceptor(...string) connect.UnaryInterceptorFunc
	// UnaryTenantInterceptor returns a Connect RPC interceptor that extracts and validates tenant context
	UnaryTenantInterceptor() connect.UnaryInterceptorFunc
	// UnaryAuthZInterceptor returns a Connect RPC interceptor that enforces authorization policies
	// using the provided AuthZ enforcer to validate user permissions for requested resources and actions
	UnaryAuthZInterceptor(enforcer AuthZ) connect.UnaryInterceptorFunc
	// UnaryTracingInterceptor returns a Connect RPC interceptor that provides OpenTelemetry tracing instrumentation
	// for distributed tracing and observability of RPC calls across services.
	UnaryTracingInterceptor() *otelconnect.Interceptor
	// UnaryRequestIDInterceptor returns a Connect RPC interceptor that generates and injects a unique request ID
	// into the context for tracking and correlating requests across distributed services.
	UnaryRequestIDInterceptor() connect.UnaryInterceptorFunc
}

// AuthZ defines the interface for authorization policy enforcement.
// It provides methods to check user permissions against resources and actions
// and to load authorization policies from a data source using the Casbin framework.
type AuthZ interface {
	// HasPermission checks if the user identified by userClaims has permission to perform
	// the specified action on the given resource. Returns true if allowed, false otherwise.
	HasPermission(userClaims *UserAuthClaims, tenant, domain, resource, action string) (bool, error)
	// Load initializes the authorization enforcer by loading the policy model and rules.
	// It enables auto-save and logging features for the enforcer.
	Load() error
}
