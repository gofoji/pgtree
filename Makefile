genNode:
	foji weld nodeProto

lint:
	golangci-lint run

test:
	go test ./...

cover:
	go test	-coverprofile cp.out ./...
	go tool cover -html=cp.out
