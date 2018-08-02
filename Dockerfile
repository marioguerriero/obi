FROM iron/go:dev

ENV SRC_DIR /go/src/obi

ENV GOOGLE_APPLICATION_CREDENTIALS /go/src/obi/dataproc-sa.json

WORKDIR /app

ADD . $SRC_DIR

RUN cd $SRC_DIR; go get .; go build -o obi; cp obi /app/

EXPOSE 8080/udp

ENTRYPOINT [ "./obi" ]
