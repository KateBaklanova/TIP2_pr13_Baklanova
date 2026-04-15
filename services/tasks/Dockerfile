# Stage 1: Builder
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Копируем корневой go.mod и go.sum
COPY go.mod go.sum ./
RUN go mod download

# Копируем весь код
COPY . .

# Собираем бинарник для tasks
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/tasks ./services/tasks/cmd/tasks

# Stage 2: Runner
FROM alpine:latest

WORKDIR /app

# Копируем бинарник из builder
COPY --from=builder /bin/tasks /app/tasks

# Открываем порт
EXPOSE 8082

# Запуск
CMD ["/app/tasks"]