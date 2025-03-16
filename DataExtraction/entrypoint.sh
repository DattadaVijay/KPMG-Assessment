#!/bin/bash

# Start PostgreSQL service
service postgresql start

# Wait for PostgreSQL to start
sleep 5

# Update PostgreSQL authentication configuration
echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/13/main/pg_hba.conf
echo "listen_addresses='*'" >> /etc/postgresql/13/main/postgresql.conf

# Restart PostgreSQL to apply changes
service postgresql restart

# Wait for PostgreSQL to restart
sleep 5

# Set password for postgres user
su - postgres -c "psql -c \"ALTER USER postgres PASSWORD 'postgres';\""

# Create swimming_olympics database if it doesn't exist
su - postgres -c "createdb -E UTF8 swimming_olympics" || true

# Initialize database schema
su - postgres -c "psql -d swimming_olympics -f /sql/init.sql" || true

# Keep container running
tail -f /dev/null
