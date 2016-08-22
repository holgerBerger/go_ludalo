BUILDID:=$(shell date '+%Y-%m-%d_%H:%M:%S')
GIT:=$(shell git rev-parse HEAD)


all: Batchcollector_alps Batchcollector_torque

Batchcollector_alps:
	go build -o batchcollector_alps -ldflags "-X main.BuildID=$(BUILDID) -X main.Hash=$(GIT)" -tags alps

Batchcollector_torque:
	go build -o batchcollector_torque -ldflags "-X main.BuildID=$(BUILDID) -X main.Hash=$(GIT)" 

clean:
	go clean 
	rm batchcollector_torque batchcollector_alps batchcollector

