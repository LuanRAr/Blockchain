# Stage 1: compilação
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY . .

ARG TARGET

# ledger fica em subdiretório próprio; broker/sensor/drone ficam na raiz
RUN go mod download && \
    if [ "$TARGET" = "ledger" ]; then \
        go build -o main ./ledger/ledger.go; \
    else \
        go build -o main ./${TARGET}/${TARGET}.go; \
    fi

# Stage 2: imagem mínima de runtime
FROM alpine:latest
WORKDIR /root/
COPY --from=builder /app/main .
EXPOSE 5000 7000
CMD ["./main"]