FROM postgres:13
ENV POSTGRES_USER: airflow
ENV POSTGRES_PASSWORD: airflow
ENV POSTGRES_DB: airflow
COPY ./sparks.sql /docker-entrypoint-initdb.d/init.sql
CMD ["docker-entrypoint.sh", "postgres"]
EXPOSE 5432
