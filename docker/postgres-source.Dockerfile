FROM postgres:15-alpine

# Install PostgreSQL contrib for additional extensions
RUN apk add --no-cache postgresql-contrib

# Set environment variables
ENV POSTGRES_DB=sourcedb
ENV POSTGRES_USER=sourceuser
ENV POSTGRES_PASSWORD=sourcepass

# Copy custom PostgreSQL configuration for CDC
COPY ./postgresql.conf /etc/postgresql/postgresql.conf

# Copy initialization scripts
COPY ./init-source-db.sql /docker-entrypoint-initdb.d/

# Set the configuration file location
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]

# Expose PostgreSQL port
EXPOSE 5432
