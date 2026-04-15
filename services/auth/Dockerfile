# Stage 1: Builder
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Копируем корневой go.mod и go.sum
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь код
COPY . .

# Собираем бинарник для auth
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/auth ./services/auth/cmd/auth

# Stage 2: Runner
FROM alpine:latest

WORKDIR /app

# Копируем бинарник из builder
COPY --from=builder /bin/auth /app/auth

# Открываем порты
EXPOSE 8081 50051

# Запуск
CMD ["/app/auth"]