FROM postgres:latest

COPY ./init.sql /docker-entrypoint-initdb.d/

ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=password123
ENV POSTGRES_DB=etl_db