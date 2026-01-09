package domecore

import (
	"errors"
	"fmt"
	"os"

	commonv1 "buf.build/gen/go/marketdome/common/protocolbuffers/go/marketdome/common/v1"
	"github.com/beego/beego/v2/core/logs"
	"github.com/casbin/casbin/v2"
	gormadapter "github.com/casbin/gorm-adapter/v3"
	"gorm.io/gorm"
)

// authZ implements the AuthZ interface and provides authorization enforcement
// using Casbin. It wraps a Casbin enforcer to check permissions against
// defined policies and resources.
type authZ struct {
	enforcer *casbin.Enforcer
	loggR    *logs.BeeLogger
}

// authZResolver implements the ResourceResolver interface and maps RPC procedures
// to their required permissions (resource and action pairs). It is used to determine
// what permissions are needed for a given procedure call.
type authZResolver struct {
	permissions map[string]*commonv1.Permission
	loggR       *logs.BeeLogger
}

type Policy struct {
	ID    uint   `gorm:"primaryKey"`
	PType string `gorm:"size:10;index:idx_ptype"`

	V0 string `gorm:"size:255;index:idx_rule"` // role
	V1 string `gorm:"size:255;index:idx_rule"` // tenant
	V2 string `gorm:"size:255;index:idx_rule"` // domain
	V3 string `gorm:"size:255;index:idx_rule"` // resource
	V4 string `gorm:"size:255;index:idx_rule"` // action
	V5 string `gorm:"size:255"`
}

// Resolve maps an RPC procedure name to its required resource and action permissions.
// It looks up the procedure in the permissions map and returns the associated resource
// and action that are required to execute the procedure.
//
// Parameters:
//   - procedure: the name of the RPC procedure to resolve
//
// Returns:
//   - resource: the name of the resource required for the procedure
//   - action: the action required to be performed on the resource
//   - error: an error if the procedure is not found in the permissions map, nil otherwise
func (a authZResolver) Resolve(procedure string) (domain, resource, action string, err error) {
	p, ok := a.permissions[procedure]
	a.loggR.Debug("Permission for procedure:", procedure, "=>", p)

	if !ok {
		return "", "", "", fmt.Errorf("permission not found for procedure: %s", procedure)
	}

	return p.Domain, p.Resource, p.Action, nil
}

// HasPermission checks if a user has permission to perform a specific action on a resource.
// It uses the Casbin enforcer to evaluate the authorization policy.
//
// Parameters:
//   - userClaims: the authenticated user's claims containing the user ID
//   - resource: the name of the resource being accessed
//   - action: the action to be performed on the resource
//
// Returns:
//   - true if the user is authorized, false otherwise
func (auth *authZ) HasPermission(userClaims *UserAuthClaims, tenant string, domain string, resource string, action string) (bool, error) {
	auth.loggR.Info(
		"AuthZ check user=%s tenant=%s domain=%s resource=%s action=%s",
		userClaims.ID, tenant, domain, resource, action,
	)

	allowed, err := auth.enforcer.Enforce(
		userClaims.ID,
		tenant,
		domain,
		resource,
		action,
	)
	if err != nil {
		return false, fmt.Errorf("failed to enforce policy: %w", err)
	}

	if !allowed {
		return false, fmt.Errorf(
			"permission denied: %s:%s on %s (tenant=%s)",
			resource, action, domain, tenant,
		)
	}

	return true, nil
}

// Load initializes the authorization enforcer by loading the model and policies.
// It also enables auto-save and logging features for the enforcer.
//
// Returns:
//   - error if loading the policy fails, nil otherwise
func (auth *authZ) Load() error {
	//auth.enforcer.EnableLog(true)
	//if err := auth.enforcer.LoadPolicy(); err != nil {
	//	return err
	//}
	//auth.enforcer.EnableAutoSave(true)

	return nil
}

// NewAuthZ creates a new authorization instance with the specified database and logger.
// It initializes a GORM adapter for Casbin policies and loads the authorization model
// from the model.conf file.
//
// Parameters:
//   - db: the GORM database connection for storing and retrieving policies
//   - logger: the logger instance for logging authorization-related messages
//
// Returns:
//   - AuthZ: a new authorization instance
//   - error: an error if the adapter creation, model loading, or enforcer initialization fails, nil otherwise
func NewAuthZ(db *gorm.DB, logger *logs.BeeLogger) (AuthZ, error) {
	logger.Info("👮🏽[AuthZ]: Setting up authorization policy enforcement...")
	a, err := gormadapter.NewAdapterByDBWithCustomTable(db, &Policy{}, "policies")
	if err != nil {
		return nil, err
	}

	if _, err := os.Stat("model.conf"); errors.Is(err, os.ErrNotExist) {
		panic("model.conf not found")
	}

	logger.Info("👮🏽[AuthZ]: Loading authorization policies...")
	enforcer, err := casbin.NewEnforcer("model.conf", a)
	if err != nil {
		return nil, err
	}

	logger.Info("👮[AuthZ]: Authorization policies loaded successfully.")
	return &authZ{
		enforcer: enforcer,
		loggR:    logger,
	}, nil
}

// NewAuthZResolver creates a new resource resolver that maps procedures to permissions.
//
// Parameters:
//   - permissions: a map of procedure names to their required permissions
//
// Returns:
//   - ResourceResolver: a new resolver instance for mapping procedures to resources and actions
func NewAuthZResolver(permissions map[string]*commonv1.Permission, logger *logs.BeeLogger) ResourceResolver {
	return &authZResolver{permissions: permissions, loggR: logger}
}
