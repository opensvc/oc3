// Package api provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen/v2 version v2.0.0 DO NOT EDIT.
package api

const (
	BasicAuthScopes  = "basicAuth.Scopes"
	BearerAuthScopes = "bearerAuth.Scopes"
)

// NodeStatus defines model for NodeStatus.
type NodeStatus = map[string]interface{}

// Problem defines model for Problem.
type Problem struct {
	// Detail A human-readable explanation specific to this occurrence of the
	// problem.
	Detail string `json:"detail"`

	// Status The HTTP status code ([RFC7231], Section 6) generated by the
	// origin server for this occurrence of the problem.
	Status int `json:"status"`

	// Title A short, human-readable summary of the problem type.  It SHOULD
	// NOT change from occurrence to occurrence of the problem, except
	// for purposes of localization (e.g., using proactive content
	// negotiation; see [RFC7231], Section 3.4).
	Title string `json:"title"`
}

// Package defines model for package.
type Package = map[string]interface{}

// System defines model for system.
type System = map[string]interface{}

// N200 defines model for 200.
type N200 = Problem

// N400 defines model for 400.
type N400 = Problem

// N401 defines model for 401.
type N401 = Problem

// N403 defines model for 403.
type N403 = Problem

// N500 defines model for 500.
type N500 = Problem

// PostDaemonStatusParams defines parameters for PostDaemonStatus.
type PostDaemonStatusParams struct {
	XDaemonChange *string `json:"XDaemonChange,omitempty"`
}

// PostDaemonStatusJSONRequestBody defines body for PostDaemonStatus for application/json ContentType.
type PostDaemonStatusJSONRequestBody = NodeStatus

// PostDaemonSystemJSONRequestBody defines body for PostDaemonSystem for application/json ContentType.
type PostDaemonSystemJSONRequestBody = System
