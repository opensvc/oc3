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

	// (POST /daemon/status)
	PostDaemonStatus(ctx echo.Context, params PostDaemonStatusParams) error

	// (POST /daemon/system)
	PostDaemonSystem(ctx echo.Context) error

	// (POST /daemon/system/package)
	PostDaemonSystemPackage(ctx echo.Context) error

	// (GET /public/openapi)
	GetSwagger(ctx echo.Context) error
}

// ServerInterfaceWrapper converts echo contexts to parameters.
type ServerInterfaceWrapper struct {
	Handler ServerInterface
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

	router.POST(baseURL+"/daemon/status", wrapper.PostDaemonStatus)
	router.POST(baseURL+"/daemon/system", wrapper.PostDaemonSystem)
	router.POST(baseURL+"/daemon/system/package", wrapper.PostDaemonSystemPackage)
	router.GET(baseURL+"/public/openapi", wrapper.GetSwagger)

}

// Base64 encoded, gzipped, json marshaled Swagger object
var swaggerSpec = []string{

	"H4sIAAAAAAAC/+yW32/bNhDH/5UDt4cW0Pyj7jbAe+qPZc02NEHtYgNsP9DUWWInkezx5NQ1/L8PJC1H",
	"cZy0wNpgA/ZkSfzy7qu7z4neCmVrZw0a9mK8FYTeWeMx3jwZDMKPsobRcLiUzlVaSdbW9N95a8Izr0qs",
	"Zbj6lnAlxuKb/nXMflr1/UuyywprsdvtMpGjV6RdCCPG4uI3scvE04dJ9lzm8AbfN+g5ZR0+RNa3RjZc",
	"WtIfMU9pRw+R9szSUuc5mpDz+4cp8LlhJCMrmCCtkeBnIksi6PabQ+zXNscJS27iHW8cirGwy3eoYlfa",
	"8OOtcGQdEutEZI4sdZWuukmfQdnU0nxHKHO5rBDwg6ukia8G3qHSK62ALXCpPVilGiI0CsGugEucG5cy",
	"9uZGZK0fz6RNEfz4g9Wbaaclwqvp9BKSAJTNER7N3py9+PHJaLjIYIIqWvjhMRRokCRjDstNymlJF9qA",
	"T3VaWbrDHZwypw1jgRTcseYKT9XEl5Y4Oy6Nb+pa0uYoOIS4PYBzhsmri7e/v5yb1xdTUKU0BcKKbN01",
	"xvZumxngB4WO5ya8kmvIWY8+iCqrZKU/pq48wl7Ry6Dx2hRhq1Ss1wh7POfGYGFZR+1P4BHhRFlHvaeP",
	"T7ZslwnC940mzMV41mJzaGRbs0V2mz0n1V+ywJNc+o3nhOXRUlhD1ZDmzSRQnmhdSq/Vs4bLw2CFPfHp",
	"teWS2YXYS5SE1KrT3ZmlWrIYi1//mIqsEyKuHscILrRZ2egvMSGsQ+PXCpStKlRsCaTTIhNrJJ8wGfQG",
	"vWEwEKRhcSxGvUFvIEIpuIwv0s8l1tb0r+fAWc+3kbu0niFp25FYa7xK+BibY2pWmOjY2PN8v+ll3DNp",
	"u+MkyRoZyYvxbCt0iF2izOM7Gxlr8Gfa8yIC2hZH3vZUac8BvkSyB48hepjDK80leCcVngBokQhCz89t",
	"vvliH87Oh293k1KmBuOD2wfwqYAHXT+Irs/P+7VPW+3wc7TDzlH1Ke2oc8Tcrw2i7sDEFndGZbbYZdsb",
	"4zBbhHawLAIOQhahC4sQ4sDlYS4/g8uohVx7ZddIm/uJTIG/Dgt71/9z8IU56Hc+4S0P9zf4cr8h+3r/",
	"f1tPn/r/+1/pWyy7a5aVVv3DybEVBZ4o9y/IkytZFPHz/Y8qfHzs3lnLfwHXLaepSAHU3e7vAAAA//9R",
	"K0avbw0AAA==",
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
