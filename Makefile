OSVC_CONTEXT =

GOCMD ?= go
GOBUILD := $(GOCMD) build
GOCLEAN := $(GOCMD) clean
GOTEST := $(GOCMD) test
GOGEN := $(GOCMD) generate
GOVET := $(GOCMD) vet

MKDIR := /bin/mkdir

DIST := dist
OC3 := $(DIST)/oc3

all: clean vet test race build

build: version api oc3

deps:
	$(GOCMD) get -tool github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@latest

api:
	$(GOGEN) ./apifeeder
	$(GOGEN) ./apicollector

clean:
	$(GOCLEAN)
	$(GOCLEAN) -testcache
	rm -f $(OC3)

oc3:
	$(MKDIR) -p $(DIST)
	$(GOBUILD) -o $(OC3) .

race:
	$(GOTEST) -p 1 -timeout 240s ./... -race

test:
	$(GOTEST) -p 1 -timeout 60s ./...

testinfo:
	TEST_LOG_LEVEL=info $(GOTEST) -p 1 -timeout 60s ./...

version:
	git describe --tags --abbrev > util/version/text/VERSION

vet:
	$(GOVET) ./...
