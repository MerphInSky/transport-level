FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/transport ./cmd/transport

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/transport /usr/local/bin/transport
USER app
EXPOSE 8080
ENTRYPOINT ["transport"]
