FROM golang:1.25

WORKDIR /app

COPY . .

RUN go build -o backend ./cmd/backend

EXPOSE 8081
CMD ["./backend"]
