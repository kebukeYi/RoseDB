build:
	go mod download
	go build -o rosedb-server.exe ./cmd/
