#!/bin/bash

# Test script for the KV Store cluster

set -e

BASE_URL="http://localhost:8080"

echo "=== KV Store Cluster Test ==="
echo ""

# Health check
echo "1. Health check..."
curl -s "$BASE_URL/health" | jq .
echo ""

# Status check
echo "2. Node status..."
curl -s "$BASE_URL/status" | jq .
echo ""

# Set a value
echo "3. Setting key 'counter' to 100..."
curl -s -X POST "$BASE_URL/kv/counter" \
  -H "Content-Type: application/json" \
  -d '{"value": 100}' | jq .
echo ""

# Wait for consensus
echo "   Waiting for consensus..."
sleep 2

# Get the value
echo "4. Getting key 'counter'..."
curl -s "$BASE_URL/kv/counter" | jq .
echo ""

# Add to the value
echo "5. Adding 50 to 'counter'..."
curl -s -X POST "$BASE_URL/kv/counter/add" \
  -H "Content-Type: application/json" \
  -d '{"value": 50}' | jq .
echo ""

sleep 2

# Get updated value
echo "6. Getting updated 'counter'..."
curl -s "$BASE_URL/kv/counter" | jq .
echo ""

# Set another key
echo "7. Setting key 'balance' to 1000..."
curl -s -X POST "$BASE_URL/kv/balance" \
  -H "Content-Type: application/json" \
  -d '{"value": 1000}' | jq .
echo ""

sleep 2

# Get all values
echo "8. Getting all key-value pairs..."
curl -s "$BASE_URL/kv" | jq .
echo ""

# Subtract from balance
echo "9. Subtracting 250 from 'balance'..."
curl -s -X POST "$BASE_URL/kv/balance/sub" \
  -H "Content-Type: application/json" \
  -d '{"value": 250}' | jq .
echo ""

sleep 2

# Check values from different node
echo "10. Checking 'balance' from node 1 (port 8081)..."
curl -s "http://localhost:8081/kv/balance" | jq .
echo ""

# Check all values from node 2
echo "11. Getting all values from node 2 (port 8082)..."
curl -s "http://localhost:8082/kv" | jq .
echo ""

echo "=== Test Complete ==="
echo ""
echo "Nodes should have consensus on the following state:"
echo "  - counter: 150 (100 + 50)"
echo "  - balance: 750 (1000 - 250)"
