BUILDID:=$(shell date '+%Y-%m-%d_%H:%M:%S')
GIT:=$(shell git rev-parse HEAD)


all: Batchcollector_alps

Batchcollector_alps:
	go build -ldflags "-X main.Id=$(BUILDID) -X main.Hash=$(GIT)" 

clean:
	go clean 

