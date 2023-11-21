FROM alpine:3.9

ENV TERM=linux

RUN apk add --no-cache bash mongodb mongodb-tools

RUN mkdir -p /data/db && \
    chown -R mongodb /data/db

# VOLUME /data/db
# EXPOSE 27017
# CMD [ "mongod", "--bind_ip", "0.0.0.0"]
