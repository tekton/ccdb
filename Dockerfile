FROM golang:buster
WORKDIR /src
COPY . /src
RUN go clean -modcache
RUN go build
EXPOSE 6379
CMD ["./ccdb"]