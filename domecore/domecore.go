package domecore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	commonv1 "buf.build/gen/go/marketdome/common/protocolbuffers/go/marketdome/common/v1"
	"connectrpc.com/connect"
	"google.golang.org/grpc/metadata"
	"gorm.io/gorm"
)

// PagedResult -------------For Pagination

func (p *PagedResult[T]) GetTotalPages(limit int32) int32 {
	if p == nil || limit <= 0 {
		return 0
	}
	return int32((p.Total + int64(limit) - 1) / int64(limit))
}

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

// UserAuthClaims represents the JWT claims structure

func (u UserAuthClaims) String() string {
	jb, _ := json.Marshal(u)
	return string(jb)
}

//Context helper for authentication

//Exceptions

var ErrMissingTenantHeader = connect.NewError(connect.CodeInvalidArgument, errors.New("x-tenant-id is required in the header"))
var ErrFailedParsingTokenClaims = connect.NewError(connect.CodeInvalidArgument, errors.New("token claims could not be parsed"))
var ErrInvalidToken = connect.NewError(connect.CodeUnauthenticated, errors.New(fmt.Sprintf("invalid token")))
var ErrMissingOrInvalidToken = connect.NewError(connect.CodeUnauthenticated, errors.New(fmt.Sprintf("missing or invalid token")))

//Helpers

type contextHelper struct {
	authenticator Authenticator
}

func (helper *contextHelper) GetAccessToken(request connect.AnyRequest) (string, error) {
	return helper.authenticator.ExtractHeaderToken(request)
}

func (helper *contextHelper) GetUserClaims(ctx context.Context) *UserAuthClaims {
	userClaims := ctx.Value(ContextKeyUser).(*UserAuthClaims)
	return userClaims
}

func (helper *contextHelper) GetTenant(ctx context.Context) (string, error) {
	// First, try to get tenant ID from context (set by UnaryTenantInterceptor for Connect-RPC)
	if tenantID := ctx.Value(XTenantKey); tenantID != nil {
		if id, ok := tenantID.(string); ok && id != "" {
			log.Printf("Retrieved tenant ID from context: %s", id)
			return id, nil
		}
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

func NewContextHelper(authenticator Authenticator) ContextHelper {
	return &contextHelper{
		authenticator: authenticator,
	}
}
