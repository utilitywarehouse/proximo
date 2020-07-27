FROM golang:1.14-alpine AS build
RUN apk --no-cache add git
WORKDIR /go/src/app
ADD . /go/src/app/
ENV CGO_ENABLED 0
RUN go get ./... \
 && go build -o /proximo-server .

FROM alpine:3.12
RUN apk add --no-cache ca-certificates
COPY --from=build /proximo-server /proximo-server
CMD [ "/proximo-server" ]
