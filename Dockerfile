FROM golang:1.24 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/router ./cmd/router && go build -o /out/mock-worker ./cmd/mock-worker

FROM gcr.io/distroless/base-debian12
COPY --from=build /out/router /usr/local/bin/router
ENTRYPOINT ["/usr/local/bin/router"]

