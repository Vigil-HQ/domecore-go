package domecore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	commonv1 "buf.build/gen/go/marketdome/common/protocolbuffers/go/marketdome/common/v1"
	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

// PagedResult represents a paginated response containing a slice of items and the total count.
// It is a generic type that can hold any type of items in the Items field.
// The Total field represents the total number of items available across all pages.
//
// Type Parameters:
//   - T: The type of items contained in the paginated result
//
// Fields:
//   - Items: The slice of items for the current page
//   - Total: The total number of items available across all pages

func (p *PagedResult[T]) GetTotalPages(limit int32) int32 {
	if p == nil || limit <= 0 {
		return 0
	}
	return int32((p.Total + int64(limit) - 1) / int64(limit))
}

// NewPagedResult creates a new PagedResult instance with the specified total count and items.
// This is a factory function for creating paginated results in a consistent way.
//
// Type Parameters:
//   - T: The type of items contained in the paginated result
//
// Parameters:
//   - total: The total number of items available across all pages
//   - items: The slice of items for the current page
//
// Returns:
//   - A pointer to a new PagedResult instance containing the provided items and total count
//
// Example Usage:
//
//	result := NewPagedResult[User](100, []User{user1, user2})
func NewPagedResult[T any](total int64, items T) *PagedResult[T] {
	return &PagedResult[T]{
		Items: items,
		Total: total,
	}
}

// WithPaginationScope creates a GORM scope function that implements pagination functionality.
// It handles page numbers, limits, and sorting of database queries.
//
// Parameters:
//   - pagination: A PageRequest object containing pagination parameters (page number, limit, sort field, sort direction)
//
// Returns:
//   - A GORM scope function that applies pagination, limiting and sorting to the query
//
// Example Usage:
//
//	db.Scopes(WithPaginationScope(&PageRequest{
//	    Page:      1,
//	    Limit:     20,
//	    Sort:      "created_at",
//	    Direction: SortDirection_SORT_DIRECTION_DESC,
//	})).Find(&records)
func WithPaginationScope(pagination *commonv1.PageRequest) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		// Defaults
		page := pagination.GetPage()
		if page <= 0 {
			page = 1
		}

		limit := pagination.GetLimit()
		if limit <= 0 {
			limit = 20
		}

		offset := (page - 1) * limit

		// Apply limit and offset
		db = db.Limit(int(limit)).Offset(int(offset))

		// Sorting
		sort := pagination.GetSort()
		direction := pagination.GetDirection()

		if sort == "" {
			sort = "created_at"
		}

		order := "desc"
		switch direction {
		case commonv1.SortDirection_SORT_DIRECTION_ASC:
			order = "asc"
		case commonv1.SortDirection_SORT_DIRECTION_DESC:
			order = "desc"
		}

		db = db.Order(fmt.Sprintf("%s %s", sort, order))

		return db
	}
}

// WithTenantScope creates a GORM scope function that filters database queries by tenant ID.
// It is used to implement multi-tenancy by ensuring that queries only return records
// belonging to the specified tenant.
//
// Parameters:
//   - tenantId: The unique identifier of the tenant to filter by
//
// Returns:
//   - A GORM scope function that adds a WHERE clause for the tenant_id field
//
// Example Usage:
//
//	db.Scopes(WithTenantScope(ctx)).Find(&records)
func WithTenantScope(ctx context.Context) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		tenantId := ctx.Value(XTenantKey)
		log.Printf("👮 [WithTenantScope]: TenantId: %s", tenantId)
		return db.Where("tenant_id = ?", tenantId)
	}
}

const (
	// ContextKeyUser is used to store the authenticated user's claims in context.
	ContextKeyUser = "UserClaimsKey"
	// XTenantKey is the metadata key for the company Id header
	XTenantKey = "x-tenant-id"
)

// UserAuthClaims represents the JWT claims structure containing authenticated user information.
// This structure holds the claims extracted from JWT tokens used for authentication and authorization.

// String returns a JSON string representation of the UserAuthClaims.
// This method is useful for logging and debugging purposes.
//
// Returns:
//   - A JSON-formatted string representation of the user claims
//
// Example Usage:
//
//	claims := UserAuthClaims{...}
//	log.Printf("User claims: %s", claims.String())
func (u UserAuthClaims) String() string {
	jb, _ := json.Marshal(u)
	return string(jb)
}

//Context helper for authentication

//Exceptions

var ErrMissingTenantHeader = connect.NewError(connect.CodeInvalidArgument, errors.New("x-tenant-id is required in the header"))
var ErrFailedParsingTokenClaims = connect.NewError(connect.CodeInvalidArgument, errors.New("token claims could not be parsed"))
var ErrInvalidToken = connect.NewError(connect.CodeUnauthenticated, errors.New("invalid token"))
var ErrMissingOrInvalidToken = connect.NewError(connect.CodeUnauthenticated, errors.New("missing or invalid token"))

//Helpers

// contextHelper is a concrete implementation of the ContextHelper interface.
// It provides utility methods for extracting authentication tokens, user claims,
// and tenant information from context and request objects.
//
// Fields:
//   - authenticator: An Authenticator instance used to extract and validate tokens
type contextHelper struct {
	authenticator Authenticator
}

// GetRequestID extracts the request ID from the context.
// It retrieves the value associated with the "x-request-id" key from the context.
// This is useful for request tracking and correlating logs across distributed systems.
//
// Parameters:
//   - ctx: A context.Context containing the request ID value
//
// Returns:
//   - The request ID as a string, or an empty string if no request ID is present in the context
//
// Example Usage:
//
//	requestID := helper.GetRequestID(ctx)
//	log.Printf("Request ID: %s", requestID)
func (helper *contextHelper) GetRequestID(ctx context.Context) string {
	requestID := ctx.Value("x-request-id")
	if requestID == nil {
		return ""
	}

	return requestID.(string)
}

// GetTraceID extracts the OpenTelemetry trace ID from the context.
// It retrieves the current span from the context and returns its trace ID as a string.
// This is useful for correlating logs and debugging distributed traces.
//
// Parameters:
//   - ctx: A context.Context containing OpenTelemetry span information
//
// Returns:
//   - The trace ID as a string, or an empty string if no span is present in the context
//
// Example Usage:
//
//	traceID := helper.GetTraceID(ctx)
//	log.Printf("Request trace ID: %s", traceID)
func (helper *contextHelper) GetTraceID(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().TraceID().IsValid() {
		return ""
	}

	return span.SpanContext().TraceID().String()
}

// GetAccessToken extracts the access token from the request headers.
// It delegates to the authenticator's ExtractHeaderToken method.
//
// Parameters:
//   - request: A connect.AnyRequest containing the HTTP request with headers
//
// Returns:
//   - The extracted access token string
//   - An error if the token cannot be extracted or is invalid
//
// Example Usage:
//
//	token, err := helper.GetAccessToken(request)
//	if err != nil {
//	    return err
//	}
func (helper *contextHelper) GetAccessToken(request connect.AnyRequest) (string, error) {
	return helper.authenticator.ExtractHeaderToken(request)
}

// GetUserClaims retrieves the authenticated user's claims from the context.
// The claims should have been previously stored in the context by authentication middleware.
//
// Parameters:
//   - ctx: A context.Context containing the user claims
//
// Returns:
//   - A pointer to UserAuthClaims containing the authenticated user's information
//
// Note: This method will panic if the context does not contain user claims or if the value
// cannot be type-asserted to *UserAuthClaims. Ensure authentication middleware runs before calling.
//
// Example Usage:
//
//	claims := helper.GetUserClaims(ctx)
//	log.Printf("User ID: %s", claims.UserId)
func (helper *contextHelper) GetUserClaims(ctx context.Context) *UserAuthClaims {
	userClaims := ctx.Value(ContextKeyUser).(*UserAuthClaims)
	return userClaims
}

// GetTenant extracts the tenant ID from the incoming gRPC metadata in the context.
// It looks for the x-tenant-id header to identify which tenant the request belongs to.
//
// Parameters:
//   - ctx: A context.Context containing incoming gRPC metadata
//
// Returns:
//   - The tenant ID string extracted from the x-tenant-id header
//   - An error if metadata cannot be extracted or the x-tenant-id header is missing
//
// Example Usage:
//
//	tenantId, err := helper.GetTenant(ctx)
//	if err != nil {
//	    return ErrMissingTenantHeader
//	}
func (helper *contextHelper) GetTenant(ctx context.Context) (string, error) {
	// First, try to get tenant ID from context (set by UnaryTenantInterceptor for Connect-RPC)
	if tenantID := ctx.Value(XTenantKey); tenantID != nil {
		if id, ok := tenantID.(string); ok && id != "" {
			log.Printf("Retrieved tenant ID from context: %s", id)
			return id, nil
		}
	}

	organizations, err := helper.GetOrganizations(ctx)
	if err == nil && len(organizations) == 1 {
		return organizations[0], nil
	}
	if err == nil && len(organizations) > 1 {
		return "", errors.New("multiple organizations found in token; x-tenant-id header is required")
	}

	// Fallback to gRPC metadata (for backward compatibility with gRPC)
	md, ok := metadata.FromIncomingContext(ctx)
	if ok {
		// Check if the X-Tenant-Id header is present in gRPC metadata
		companyID := md[XTenantKey]
		if len(companyID) > 0 && companyID[0] != "" {
			log.Printf("Received X-Tenant-Id from gRPC metadata: %s", companyID[0])
			return companyID[0], nil
		}
	}

	return "", errors.New("could not extract tenant id: x-tenant-id not found in context or metadata")
}

func (helper *contextHelper) GetOrganizations(ctx context.Context) ([]string, error) {
	claims := helper.GetUserClaims(ctx)
	if claims == nil {
		return nil, errors.New("user claims not found in context")
	}
	organizations := claims.Organization.Aliases()
	if len(organizations) == 0 {
		return nil, errors.New("no organizations found in token claims")
	}
	return organizations, nil
}

// NewContextHelper creates a new ContextHelper instance with the provided authenticator.
// This is a factory function for creating context helpers that can extract authentication
// and tenant information from requests and contexts.
//
// Parameters:
//   - authenticator: An Authenticator implementation used for token extraction and validation
//
// Returns:
//   - A ContextHelper implementation that can extract tokens, user claims, and tenant information
//
// Example Usage:
//
//	authenticator := NewJWTAuthenticator(config)
//	contextHelper := NewContextHelper(authenticator)
func NewContextHelper(authenticator Authenticator) ContextHelper {
	return &contextHelper{
		authenticator: authenticator,
	}
}
