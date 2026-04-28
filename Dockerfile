FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -o hello-websocket .

FROM alpine:3.20
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=builder /app/hello-websocket .
EXPOSE 8080
CMD ["./hello-websocket"]
