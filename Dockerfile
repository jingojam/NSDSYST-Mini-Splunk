FROM python:3.10.20-slim

WORKDIR /app

RUN pip3 install grpcio grpcio-tools
RUN pip3 install redis[hiredis]
RUN pip3 install --no-cache-dir "elasticsearch==7.17.9"

COPY . .