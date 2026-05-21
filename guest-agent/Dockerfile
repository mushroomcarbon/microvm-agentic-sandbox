FROM golang:1.25 AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/agent .

FROM ubuntu:24.04
COPY --from=builder /out/agent /usr/local/bin/agent
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/agent"]
