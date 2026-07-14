FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /app/volt-cache-server ./cmd/server

FROM alpine:3.18
RUN apk add --no-cache ca-certificates
COPY --from=build /app/volt-cache-server /usr/local/bin/volt-cache-server
WORKDIR /data
EXPOSE 6379
ENTRYPOINT ["volt-cache-server"]
