#!/bin/bash
set -e

echo "=== CREATE PG USER/DB ==="
su - postgres -c "psql -c \"CREATE USER barrage WITH PASSWORD 'barrage';\"" 2>&1 || echo "User already exists"
su - postgres -c "psql -c \"CREATE DATABASE douyin_barrage OWNER barrage;\"" 2>&1 || echo "DB already exists"
su - postgres -c "psql -c \"GRANT ALL PRIVILEGES ON DATABASE douyin_barrage TO barrage;\"" 2>&1

echo ""
echo "=== ALLOW PASSWORD AUTH ==="
PG_HBA=$(find /etc/postgresql -name pg_hba.conf 2>/dev/null | head -1)
echo "pg_hba.conf: $PG_HBA"
if [ -n "$PG_HBA" ]; then
    # Ensure md5/scram auth for TCP connections
    sed -i 's|host\s\+all\s\+all\s\+127.0.0.1/32\s\+peer|host    all             all             127.0.0.1/32            md5|' "$PG_HBA"
    sed -i 's|host\s\+all\s\+all\s\+::1/128\s\+peer|host    all             all             ::1/128                 md5|' "$PG_HBA"
    systemctl reload postgresql
    echo "pg_hba.conf updated"
fi

echo ""
echo "=== INSTALL psycopg2 ==="
pip3 install psycopg2-binary 2>&1 | tail -5

echo ""
echo "=== VERIFY PG CONNECTION ==="
PGPASSWORD=barrage psql -h 127.0.0.1 -U barrage -d douyin_barrage -c "SELECT 1 AS ok;"

echo ""
echo "=== SETUP COMPLETE ==="
