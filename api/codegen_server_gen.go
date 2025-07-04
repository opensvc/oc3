// Package api provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/oapi-codegen/oapi-codegen/v2 version v2.4.1 DO NOT EDIT.
package api

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"
)

// ServerInterface represents all server handlers.
type ServerInterface interface {

	// (POST /feed/daemon/ping)
	PostFeedDaemonPing(ctx echo.Context) error

	// (POST /feed/daemon/status)
	PostFeedDaemonStatus(ctx echo.Context) error

	// (POST /feed/node/disk)
	PostFeedNodeDisk(ctx echo.Context) error

	// (POST /feed/object/config)
	PostFeedObjectConfig(ctx echo.Context) error

	// (POST /feed/system)
	PostFeedSystem(ctx echo.Context) error

	// (GET /public/openapi)
	GetSwagger(ctx echo.Context) error

	// (GET /version)
	GetVersion(ctx echo.Context) error
}

// ServerInterfaceWrapper converts echo contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler ServerInterface
}

// PostFeedDaemonPing converts echo context to params.
func (w *ServerInterfaceWrapper) PostFeedDaemonPing(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostFeedDaemonPing(ctx)
	return err
}

// PostFeedDaemonStatus converts echo context to params.
func (w *ServerInterfaceWrapper) PostFeedDaemonStatus(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostFeedDaemonStatus(ctx)
	return err
}

// PostFeedNodeDisk converts echo context to params.
func (w *ServerInterfaceWrapper) PostFeedNodeDisk(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostFeedNodeDisk(ctx)
	return err
}

// PostFeedObjectConfig converts echo context to params.
func (w *ServerInterfaceWrapper) PostFeedObjectConfig(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostFeedObjectConfig(ctx)
	return err
}

// PostFeedSystem converts echo context to params.
func (w *ServerInterfaceWrapper) PostFeedSystem(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostFeedSystem(ctx)
	return err
}

// GetSwagger converts echo context to params.
func (w *ServerInterfaceWrapper) GetSwagger(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.GetSwagger(ctx)
	return err
}

// GetVersion converts echo context to params.
func (w *ServerInterfaceWrapper) GetVersion(ctx echo.Context) error {
	var err error

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.GetVersion(ctx)
	return err
}

// This is a simple interface which specifies echo.Route addition functions which
// are present on both echo.Echo and echo.Group, since we want to allow using
// either of them for path registration
type EchoRouter interface {
	CONNECT(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	DELETE(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	GET(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	HEAD(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	OPTIONS(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	PATCH(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	POST(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	PUT(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
	TRACE(path string, h echo.HandlerFunc, m ...echo.MiddlewareFunc) *echo.Route
}

// RegisterHandlers adds each server route to the EchoRouter.
func RegisterHandlers(router EchoRouter, si ServerInterface) {
	RegisterHandlersWithBaseURL(router, si, "")
}

// Registers handlers, and prepends BaseURL to the paths, so that the paths
// can be served under a prefix.
func RegisterHandlersWithBaseURL(router EchoRouter, si ServerInterface, baseURL string) {

	wrapper := ServerInterfaceWrapper{
		Handler: si,
	}

	router.POST(baseURL+"/feed/daemon/ping", wrapper.PostFeedDaemonPing)
	router.POST(baseURL+"/feed/daemon/status", wrapper.PostFeedDaemonStatus)
	router.POST(baseURL+"/feed/node/disk", wrapper.PostFeedNodeDisk)
	router.POST(baseURL+"/feed/object/config", wrapper.PostFeedObjectConfig)
	router.POST(baseURL+"/feed/system", wrapper.PostFeedSystem)
	router.GET(baseURL+"/public/openapi", wrapper.GetSwagger)
	router.GET(baseURL+"/version", wrapper.GetVersion)

}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{

	"H4sIAAAAAAAC/+xZW2/buBL+KwTPeWgB1Xbi9BzA56mXk213F01Qp7sPSWDQ1FhmK5EsOUriBv7vC5KS",
	"LUX0JdkkWGD3KbI4mss33wyHzC3lqtBKgkRLR7fUgNVKWvA/jgYD94criSDRPTKtc8EZCiX7X62S7p3l",
	"cyiYe/q3gRkd0X/11zr7YdX2T42a5lDQ5XKZ0BQsN0I7NXRE37KUfIbvJViky4QeDQ6ew+oXyUqcKyN+",
	"QBrMDp/D7LEyU5GmIJ3N188D8EeJYCTLyRjMFRjyf2OUcfaPAdL3DAolT4XM3nAOGiG9l0vaKA0GRWCM",
	"mn4FjpNrgXNV4oQrOROZW2g7lAuLRM1IECeSFWAJzhkSA99LYcCS05PxGekrPuzPANJ+kOxXChMqEArb",
	"VdxQSBOKCw10RC0aITMXb/WCGcMWdLl+ET6LQZd6dIhFhqUlKAqwyAptybXIczIFYmBmwM4hJTNliFQp",
	"tIEd+y//gXYXtNvwXCYVKt6t98J+68KTeiw6bok0+rpQKeTRlQpmzXAeXTeQiZCozpIVP8AtzJQpGNIR",
	"FRKHh2uwhETIwBdeaaHpWGPlCmSqTES/t+0zmNLRuQus7Wxlv9K9UlTHmjiELjtpSegnlYKD1EYwZeiJ",
	"uGLEtv7j07IHERJ64p/erQjcNsq0jqLLVVFUZdNZS42eeKpsW7StUHYQOKEgr6JysxxuJgW7iacvrAq5",
	"ZRWZyQDjAoWSApWBdGLAqtJwmHBVyg3SyvA5WDQM45FvJjG7bjSQFV+nC4zWtuVKw/3QQ6VVrrLFbiJ7",
	"J2PEPFUW2xtUlyyrvMabIM9Li1C1kY2drSm1Z3+rS28P281G/Ejd1RW3sVUbauvBORClQdorTnguQCJx",
	"VUzqD5Id2QiArsNbm9qdorDVdZPE50xmd8ovGr0yREiLTPK9QKjbU8ctbeBKqNJOSp0yhHTCsMVz9/KV",
	"28tjVh7yzZOlw4eYrBDckY1qAuz2cUAm8q53b8i8LJh8ZYClbJoDgRudM+nnEWI1cDETnKAiOBeWKM5L",
	"Y0BycPTGOVxIHSz2LmS0baz40DZ7Ngfy4ezstN76uau7F+efj9/993B4cJmQMXDvwn9ekgwkuP6Wkuki",
	"2FRGZEISG0ZZNyTEvSMx5xrNEwXmEMPEzpXB5C40tiwKZhZ3lBOnt0fIRyTjDydffn1/IT+dnJGQLzIz",
	"qmg6hmqzmwmBGzcjXkgXki6NVhasE8oVZ7n4EbLyAnpZLyGlFTJznzKO4gpINVNeSAmZQuFl/0csAInA",
	"OuwdvYym7C75Am1Wiawxi3FPM/6NZRDZzg2P70G+0vP8npXmG2R8+Mq2bEy3W2t2R0sMPXldsj6kSnMw",
	"HEPELizGirGB1F5zVS2/12jVCKpttbEAN6zQjvp00Bv0DnbSYHPLcVECL43Axdh5G0xNmRX8TRlGDx+F",
	"+8a/XduaI2rn8BSYAVNLh1/HNRN+/v2MJg0VfvWujqXn0kz5RIaapquWq/IcOCpDmBaNHI7oQW/Qe+33",
	"cQ3SLY7osDfoDWiYmnwg4XgUjil9Xc8fymK3a3wOp5bVpr/p2OirzuXFV+jHlI5iY07AHyy+Veni8S4I",
	"uoaW7VyjKcG/aNwBHQ4ONyleyfU33CMsE3o4OOqiVQjr+1cbpfq8l1QH1Ar4alVYsnK0STw6Om9R7vxy",
	"mdy2aHV+uXTUZZl1bGaZA/LSqWjld71ZPSDDO9M6rjvo0ye2MvX4qb1zk+GvzAa7FTih9a3eLtmDxlXc",
	"Ltlh4wptu6wTelTSOJ720/oiIkqYL36OdDMtN+Ce/PHCfeMmHnf2KgNdtpCnPps/EXHWR//92dIOclNM",
	"3eucx8W/fWe1fw6qg8a++LduKp4mBy0TD01DfWp63iQ0Jpwo+g5FEoQcR7i6ArPYgvY46HsanCtnH4pw",
	"HYY7vG2/+v1bNkVdTnPB+6th6pZWV1ztTP8EOL5mWeanuDug3+8/ITtvl09++csAXAMWQKoQawzkFVRt",
	"/w1gaaSbWxuXBR00f1st/Sk0txVObX0jxg/CgjPNpiIX/nxyuQw8dCd662lYmpyOaF/xIV1eLv8IAAD/",
	"/5kWvVelHAAA",
}

// GetSwagger returns the content of the embedded swagger specification file
// or error if failed to decode
func decodeSpec() ([]byte, error) {
	zipped, err := base64.StdEncoding.DecodeString(strings.Join(swaggerSpec, ""))
	if err != nil {
		return nil, fmt.Errorf("error base64 decoding spec: %w", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(zipped))
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %w", err)
	}
	var buf bytes.Buffer
	_, err = buf.ReadFrom(zr)
	if err != nil {
		return nil, fmt.Errorf("error decompressing spec: %w", err)
	}

	return buf.Bytes(), nil
}

var rawSpec = decodeSpecCached()

// a naive cached of a decoded swagger spec
func decodeSpecCached() func() ([]byte, error) {
	data, err := decodeSpec()
	return func() ([]byte, error) {
		return data, err
	}
}

// Constructs a synthetic filesystem for resolving external references when loading openapi specifications.
func PathToRawSpec(pathToFile string) map[string]func() ([]byte, error) {
	res := make(map[string]func() ([]byte, error))
	if len(pathToFile) > 0 {
		res[pathToFile] = rawSpec
	}

	return res
}

// GetSwagger returns the Swagger specification corresponding to the generated code
// in this file. The external references of Swagger specification are resolved.
// The logic of resolving external references is tightly connected to "import-mapping" feature.
// Externally referenced files must be embedded in the corresponding golang packages.
// Urls can be supported but this task was out of the scope.
func GetSwagger() (swagger *openapi3.T, err error) {
	resolvePath := PathToRawSpec("")

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = true
	loader.ReadFromURIFunc = func(loader *openapi3.Loader, url *url.URL) ([]byte, error) {
		pathToFile := url.String()
		pathToFile = path.Clean(pathToFile)
		getSpec, ok := resolvePath[pathToFile]
		if !ok {
			err1 := fmt.Errorf("path not found: %s", pathToFile)
			return nil, err1
		}
		return getSpec()
	}
	var specData []byte
	specData, err = rawSpec()
	if err != nil {
		return
	}
	swagger, err = loader.LoadFromData(specData)
	if err != nil {
		return
	}
	return
}
