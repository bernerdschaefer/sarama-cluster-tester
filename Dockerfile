FROM golang:1.8 AS build
ENV GOPATH /go

RUN \
    wget -qO - http://packages.confluent.io/deb/3.3/archive.key | apt-key add - \
    && apt-get update \
    && apt-get install -y software-properties-common \
    && add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.3 stable main" \
    && apt-get update \
    && apt-get install -y ca-certificates librdkafka-dev \
    && apt-get clean

ADD .  $GOPATH/src/github.com/cyx/sarama-cluster-tester

RUN cd $GOPATH/src/github.com/cyx/sarama-cluster-tester && go build

# FROM ubuntu:xenial
# RUN mkdir /app \
#     && useradd -M app -d /app \
#     && chown app:app /app
# USER app
# WORKDIR /app
# COPY --from=build /go/src/github.com/cyx/sarama-cluster-tester /app
