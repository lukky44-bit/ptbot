FROM grafana/k6:latest AS k6bin
FROM golang:1.25

WORKDIR /app

COPY . .

RUN go build -o app ./cmd/runner

COPY --from=k6bin /usr/bin/k6 /usr/local/bin/k6

EXPOSE 8080
CMD ["./app"]