#!/usr/bin/env bash
# Starts supplier + 3 gateway instances. Redis and nginx run in Docker.
#
# Port map:
#   4000  supplier (moved off 4001 so gateways can use 4001-4003)
#   4001  gateway 1
#   4002  gateway 2
#   4003  gateway 3
#   4100  nginx LB  -> fans out to 4001-4003
#   6379  redis

cd "$(dirname "$0")"

# Kill by PORT, not by command line. These are Windows processes: Git Bash's
# pkill cannot see their command lines, so pattern matching silently matches
# nothing and the new instances then die on EADDRINUSE.
for port in 4000 4001 4002 4003; do
  powershell.exe -NoProfile -Command "Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id \$_.OwningProcess -Force -ErrorAction SilentlyContinue }" 2>/dev/null
done
sleep 2

PORT=4000 node src/supplier.js > /tmp/sd-supplier.log 2>&1 &
echo "supplier  pid=$!"
sleep 1

for p in 4001 4002 4003; do
  PORT=$p INSTANCE_ID="gw-$p" SUPPLIER_URL="http://localhost:4000" \
    node src/gateway.js > "/tmp/sd-gw-$p.log" 2>&1 &
  echo "gateway$p pid=$!"
done

sleep 2
echo "--- health ---"
for p in 4001 4002 4003; do curl -s "http://localhost:$p/health"; echo; done
