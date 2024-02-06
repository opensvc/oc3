package main

import (
	"encoding/xml"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"
)

type Arith struct{}

type Request struct {
	MethodName string `xml:"methodName"`
	Params     []any  `xml:"params>param>value"`
}

type Args struct {
	A int `xml:"a"`
	B int `xml:"b"`
}

type Response struct {
	Params ResponseParams `xml:"params"`
}

type ResponseParams struct {
	Param ResponseParamsParam `xml:"param"`
}

type ResponseParamsParam struct {
	Value any `xml:"value"`
}

type ErrorResponse struct {
	Fault ErrorResponseFault `xml:"fault"`
}

type ErrorResponseFault struct {
	Value ErrorResponseFaultValue `xml:"value"`
}

type ErrorResponseFaultValue struct {
	Struct ErrorResponseFaultValueStruct `xml:"struct"`
}

type ErrorResponseFaultValueStruct struct {
	Member []ErrorResponseFaultValueStructMember `xml:"member"`
}

type ErrorResponseFaultValueStructMember struct {
	Name  string                                   `xml:"name"`
	Value ErrorResponseFaultValueStructMemberValue `xml:"value"`
}

type ErrorResponseFaultValueStructMemberValue struct {
	Int    int    `xml:"int"`
	String string `xml:"string"`
}

func NewErrorResponse(code int, message string) ErrorResponse {
	return ErrorResponse{
		Fault: ErrorResponseFault{
			Value: ErrorResponseFaultValue{
				Struct: ErrorResponseFaultValueStruct{
					Member: []ErrorResponseFaultValueStructMember{
						{Name: "faultCode", Value: ErrorResponseFaultValueStructMemberValue{Int: code}},
						{Name: "faultString", Value: ErrorResponseFaultValueStructMemberValue{String: message}},
					},
				},
			},
		},
	}
}

func NewResponse(data any) Response {
	return Response{
		Params: ResponseParams{
			Param: ResponseParamsParam{
				Value: data,
			},
		},
	}
}

func updateAsset(args []any) string {
	return "ok"
}

func insertGeneric(args []any) string {
	return "ok"
}

func registerXMLRPC(e *echo.Echo) {
	e.POST("/xmlrpc", func(c echo.Context) error {
		var request Request
		dec := xml.NewDecoder(c.Request().Body)
		if err := dec.Decode(&request); err != nil {
			s := fmt.Sprintf("decoding XML-RPC request: %s", err)
			slog.Error(s)
			response := NewErrorResponse(http.StatusBadRequest, s)
			return c.XML(http.StatusBadRequest, response)
		}
		slog.Info("XML-RPC call: " + request.MethodName)
		switch request.MethodName {
		case "update_asset":
			data := updateAsset(request.Params)
			response := NewResponse(data)
			return c.XML(http.StatusOK, response)
		case "insert_generic":
			data := insertGeneric(request.Params)
			response := NewResponse(data)
			return c.XML(http.StatusOK, response)
		default:
			s := fmt.Sprintf("XML-RPC method %s is not implemented: %s", request.MethodName)
			slog.Error(s)
			response := NewErrorResponse(http.StatusNotFound, s)
			return c.XML(http.StatusNotFound, response)
		}
		return c.XML(http.StatusOK, NewResponse(nil))
	})
}
