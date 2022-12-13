ARG ARCH="amd64"
ARG OS="linux"

FROM golang:1.18-buster
ADD ./mysqld_exporter /build
WORKDIR /build
RUN make common-build


FROM quay.io/prometheus/busybox-${OS}-${ARCH}:latest
LABEL maintainer="The Prometheus Authors <prometheus-developers@googlegroups.com>"
COPY --from=0 /build/mysqld_exporter /bin/mysqld_exporter

EXPOSE      9104
USER        nobody
ENTRYPOINT  [ "/bin/mysqld_exporter" ]
