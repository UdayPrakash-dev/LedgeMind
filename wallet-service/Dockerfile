# --- build stage ---
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /wallet-service .

# --- run stage ---
FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /wallet-service /wallet-service
EXPOSE 8080
ENTRYPOINT ["/wallet-service"]
