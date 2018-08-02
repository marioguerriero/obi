FROM golang

ENV SRC_DIR /go/src/obi

WORKDIR /app

ADD . $SRC_DIR

RUN cd $SRC_DIR; go get .; go build -o obi; cp obi /app/

EXPOSE 8080/udp

ENTRYPOINT [ "./obi" ]