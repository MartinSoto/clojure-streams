FROM alpine as build

RUN apk add --no-cache bash g++ make git curl python zlib-dev ca-certificates openssl

WORKDIR /build
RUN git clone https://github.com/edenhill/kafkacat.git

WORKDIR /build/kafkacat
RUN sed -i -e 's%\./configure%./configure --prefix=/usr%g' ./bootstrap.sh
RUN sed -i -e 's%/usr/local%/usr%g' ./bootstrap.sh
RUN ./bootstrap.sh
RUN DESTDIR=/dest make install


FROM alpine

RUN apk add --no-cache zlib

COPY --from=build /dest /

COPY entrypoint.sh /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
