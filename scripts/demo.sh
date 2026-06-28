#!/bin/bash
set -e

echo "=== Cloud P2P Agent Demo ==="
echo ""

# 1. Prepare a test file (~2MB)
echo "generating 2MB test file..."
dd if=/dev/urandom of=/tmp/demo_file.bin bs=1M count=2 2>/dev/null
./scripts/prepare_file.sh /tmp/demo_file.bin > /tmp/file_info.txt
cat /tmp/file_info.txt

HASH=$(grep "hash:" /tmp/file_info.txt | cut -d' ' -f2)
SIZE=$(grep "size:" /tmp/file_info.txt | cut -d' ' -f2)

echo ""
echo "=== Starting swarm (tracker + fileserver + 3 agents) ==="
docker compose down -v 2>/dev/null || true
docker compose up -d
sleep 8

echo ""
echo "=== Tracker status ==="
curl -s http://localhost:8000/health

echo ""
echo "=== Registering file with agent_a (seeder) ==="
docker compose restart agent_a
sleep 5

echo ""
echo "=== Tracker now knows about the file ==="
curl -s http://localhost:8000/files

echo ""
echo "=== Peers seeding the file ==="
curl -s http://localhost:8000/peers/$HASH

echo ""
echo "=== agent_b downloading (leecher 1) ==="
docker compose exec agent_b bash -c \
  "P2P_STORAGE_ROOT=/data/agent_b python -m agent.cli download $HASH $SIZE" &

echo ""
echo "=== agent_c downloading concurrently (leecher 2) ==="
docker compose exec agent_c bash -c \
  "P2P_STORAGE_ROOT=/data/agent_c python -m agent.cli download $HASH $SIZE" &

wait
echo ""
echo "=== Demo complete ==="