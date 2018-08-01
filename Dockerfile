FROM golang

ENV SRC_DIR=/go/src/obi

ADD . $SRC_DIR

RUN cd $SRC_DIR; go get .; go build -o obi; cp obi /

CMD [ "/obi" ]
