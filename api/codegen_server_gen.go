// Package api provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen/v2 version v2.0.0 DO NOT EDIT.
package api

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/labstack/echo/v4"
	"github.com/oapi-codegen/runtime"
)

// ServerInterface represents all server handlers.
type ServerInterface interface {

	// (POST /daemon/ping)
	PostDaemonPing(ctx echo.Context) error

	// (POST /daemon/status)
	PostDaemonStatus(ctx echo.Context, params PostDaemonStatusParams) error

	// (POST /daemon/system)
	PostDaemonSystem(ctx echo.Context) error

	// (POST /daemon/system/package)
	PostDaemonSystemPackage(ctx echo.Context) error

	// (GET /public/openapi)
	GetSwagger(ctx echo.Context) error

	// (GET /version)
	GetVersion(ctx echo.Context) error
}

// ServerInterfaceWrapper converts echo contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler ServerInterface
}

// PostDaemonPing converts echo context to params.
func (w *ServerInterfaceWrapper) PostDaemonPing(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostDaemonPing(ctx)
	return err
}

// PostDaemonStatus converts echo context to params.
func (w *ServerInterfaceWrapper) PostDaemonStatus(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Parameter object where we will unmarshal all parameters from the context
	var params PostDaemonStatusParams

	headers := ctx.Request().Header
	// ------------- Optional header parameter "XDaemonChange" -------------
	if valueList, found := headers[http.CanonicalHeaderKey("XDaemonChange")]; found {
		var XDaemonChange string
		n := len(valueList)
		if n != 1 {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Expected one value for XDaemonChange, got %d", n))
		}

		err = runtime.BindStyledParameterWithLocation("simple", false, "XDaemonChange", runtime.ParamLocationHeader, valueList[0], &XDaemonChange)
		if err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, fmt.Sprintf("Invalid format for parameter XDaemonChange: %s", err))
		}

		params.XDaemonChange = &XDaemonChange
	}

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostDaemonStatus(ctx, params)
	return err
}

// PostDaemonSystem converts echo context to params.
func (w *ServerInterfaceWrapper) PostDaemonSystem(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostDaemonSystem(ctx)
	return err
}

// PostDaemonSystemPackage converts echo context to params.
func (w *ServerInterfaceWrapper) PostDaemonSystemPackage(ctx echo.Context) error {
	var err error

	ctx.Set(BasicAuthScopes, []string{})

	ctx.Set(BearerAuthScopes, []string{})

	// Invoke the callback with all the unmarshaled arguments
	err = w.Handler.PostDaemonSystemPackage(ctx)
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

	router.POST(baseURL+"/daemon/ping", wrapper.PostDaemonPing)
	router.POST(baseURL+"/daemon/status", wrapper.PostDaemonStatus)
	router.POST(baseURL+"/daemon/system", wrapper.PostDaemonSystem)
	router.POST(baseURL+"/daemon/system/package", wrapper.PostDaemonSystemPackage)
	router.GET(baseURL+"/public/openapi", wrapper.GetSwagger)
	router.GET(baseURL+"/version", wrapper.GetVersion)

}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{

	"H4sIAAAAAAAC/+xYW3PbNhP9Kxh830Myw+piue0M+5RL07jtxBpLaTtj6wECVyRSEkCApWzFw//eWUCU",
	"qZuTaWxPH/JkkVjsOTh7ACx9y6WprNGg0fP0ljvw1mgP4eFkMKA/0mgEjfRTWFsqKVAZ3f/gjaZ3XhZQ",
	"Cfr1fwcLnvL/9e9y9uOo74+dmZdQ8aZpEp6Bl05ZSsNTfv4bbxJ++jRgL0XGLuBjDR4j6vApUN9rUWNh",
	"nPoEWYQdPQXsG+PmKstAE+b3TyPwmUZwWpRsAm4Jjv3snHGc4taTKfc7k8EEBdbhCVcWeMrN/APIUJU2",
	"fXrLrTMWHKroyAxQqDL+6oK+YEVdCf2dA5GJeQkMbmwpdFga8xakWijJ0DAslGdGyto50BKYWTAs4Erb",
	"iNi70jxp+Xh0SufEx2+obsNOC2Bvp9MxiwFMmgzYs8uLN69+PBkNZwmbgAwUfnjOctDgBELG5quIaZzK",
	"lWY+6rQw7gg7doic0gg5OGKHCks4pIkvjMNkVxpfV5Vwq53kjPL2GDtDNnl7/v7311f63fmUyULoHNjC",
	"mapLDM1xmgmDGwkWrzQtydbOGg+egkojRak+xao8g17eS1jtlc5pqpColsDW9rzSGnKDKsT+xDwAOyDr",
	"qHf6/GDJmoQ7+FgrBxlPL1vbbArZajZL9r1nhfxb5HDQl37lMdpyb2gJzqu4hbYt2xmAG1FZKhUf9Aa9",
	"4Wdpt1P3aRIXkLVTuJrQropQc+GVfFFjsdnINCe8vcMqEC0RnoNw4Nro+PTGuEogT/mvf0550kkRRndz",
	"EAulFyboET3IjQXtl5JJU5Yg0TgmrOIdefiwN+gNiACF0mDKR+EVSY9FWEg/E1AZ3bekCilqPO4b/AIW",
	"DnzB4EZ5JBfFWe1mRFWBR1FZH/aWNhkEq1Bxgq3OMp7ysfH4OswbE1iyewOe7OO6Ne5RuGtVlmwObB0I",
	"2YYArftkcLqfs1Le7y+hnZaw8flkylpZ1qPKs41buo7g6eWWFy5nTXK7Ve/LWUOeErknm4mc7oMZpdhG",
	"OC49qbbDdangOp4TxLh3r9STdhta4UQFCM4H0opyFyCyYDYtgvn+inNehZOodaXY51Qqj3TKxCPLMw+U",
	"nQ7ca4UF81ZIOLDlZnHPgceXJls92A3ZueGa7X2NroYmOdhpHUq4ietT0F2jdH/saRs7/JLYYacn+Vzs",
	"qNNL3B9LQQ/ty80B/AW+DLEsU16aJbjV/Y6MiR/HC2vW33zwwD7od+7q1g/3F3i8nvA4dW7pfCv01xfa",
	"1vNSyf6mSbjlORwo8C+Ak2uR5+HC+KqP190O6+hn6n9A4FawKNJasU6juZZqt23B2mnqx1gbmuyr+cdm",
	"6NH+FdCiH9X4X2khhRVzVarQd8+a6EP6soqtRe1KnvK+kSPezJp/AgAA//9nAsW49xAAAA==",
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
