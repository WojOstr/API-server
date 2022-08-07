# syntax=docker/dockerfile:1
FROM python:3.8-slim-buster
COPY requirements.txt .
WORKDIR /app
RUN pip3 install -r requirements.txt
COPY . .
ENTRYPOINT [ "python" ]