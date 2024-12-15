# Build stage
FROM golang:1.23-alpine AS build

# Build your Go binary
WORKDIR /app
COPY . .
RUN go build -o og-task-template main.go

# Final minimal image
FROM scratch

# Copy CA certificates
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy og-task-grype binary
COPY --from=build /app/og-task-template /og-task-template