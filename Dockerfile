FROM golang:1.17-alpine

WORKDIR /csvFileToDBapp

COPY . /csvFileToDBapp

# Install any dependencies required by the application
RUN apk update && apk add git
RUN go mod download

# Build the application
RUN go build -o main .

# Expose port 8080 to the outside world
EXPOSE 1321

# Run the application
CMD ["./main"]
