FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -o /fake-rail .

FROM alpine:3.20
RUN adduser -D -u 10001 app
USER app
COPY --from=build /fake-rail /fake-rail
EXPOSE 9000
ENTRYPOINT ["/fake-rail"]
