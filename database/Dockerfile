FROM postgres:13
ENV POSTGRES_USER: airflow
ENV POSTGRES_PASSWORD: airflow
ENV POSTGRES_DB: airflow
COPY ./sparks.sql /docker-entrypoint-initdb.d/
EXPOSE 5432