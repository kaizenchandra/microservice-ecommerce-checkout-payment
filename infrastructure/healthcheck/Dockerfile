FROM golang:1.24.2-alpine AS probe
WORKDIR /src
COPY infrastructure/healthcheck/main.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /healthcheck main.go

FROM otel/opentelemetry-collector-contrib:0.123.0 AS collector
COPY --from=probe /healthcheck /healthcheck

FROM grafana/tempo:2.7.2 AS tempo
COPY --from=probe /healthcheck /healthcheck

FROM ghcr.io/kafbat/kafka-ui:v1.3.0 AS kafka-ui
COPY --from=probe /healthcheck /healthcheck
