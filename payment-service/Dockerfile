FROM golang:1.24.2-alpine AS probe
WORKDIR /src
COPY infrastructure/healthcheck/main.go .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /healthcheck main.go

FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=probe /healthcheck /healthcheck
COPY --from=build /workspace/payment-service/target/payment-service-1.0.0-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8086
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/app.jar"]
