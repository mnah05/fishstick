FROM golang:1.27.3-alpine AS source
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

FROM source AS build
RUN mkdir -p /out && \
    for app in api schedular relay worker migrate; do \
      CGO_ENABLED=0 go build -trimpath -o /out/$app ./cmd/$app; \
    done

FROM alpine:3.22 AS runtime
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /app
COPY --from=build /out /app
USER 65532:65532
CMD ["/app/api"]

FROM source AS test
CMD ["go", "test", "-count=1", "-v", "./..."]
