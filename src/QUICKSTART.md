# KV Store Cluster Quick Start

This guide shows how to run a 5-node HotStuff BFT consensus cluster with isolated TikV storage.

## Prerequisites

- Docker and Docker Compose
- curl and jq (for testing)

## Start the Cluster

```bash
# Build and start all services
docker-compose up -d --build

# Watch logs (all nodes)
docker-compose logs -f

# Watch logs (specific node)
docker-compose logs -f kvnode0
```

## Architecture

Each node has:
- **kvnode{N}**: HotStuff consensus node with REST API
- **pd{N}**: TikV Placement Driver (isolated per node)
- **tikv{N}**: TikV storage server (isolated per node)

Port mapping:
| Node | API Port | libp2p Port | PD Port |
|------|----------|-------------|---------|
| 0    | 8080     | 4000        | 2379    |
| 1    | 8081     | 4001        | 2380    |
| 2    | 8082     | 4002        | 2381    |
| 3    | 8083     | 4003        | 2382    |
| 4    | 8084     | 4004        | 2383    |

## REST API

### Health Check
```bash
curl http://localhost:8080/health
```

### Node Status
```bash
curl http://localhost:8080/status
```

### Get a Value
```bash
curl http://localhost:8080/kv/mykey
```

### Set a Value
```bash
curl -X POST http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"value": 100}'
```

### Add to a Value
```bash
curl -X POST http://localhost:8080/kv/mykey/add \
  -H "Content-Type: application/json" \
  -d '{"value": 50}'
```

### Subtract from a Value
```bash
curl -X POST http://localhost:8080/kv/mykey/sub \
  -H "Content-Type: application/json" \
  -d '{"value": 25}'
```

### Delete a Key
```bash
curl -X DELETE http://localhost:8080/kv/mykey
```

### Get All Values
```bash
curl http://localhost:8080/kv
```

### Generic Transaction
```bash
curl -X POST http://localhost:8080/tx \
  -H "Content-Type: application/json" \
  -d '{"type": "set", "key": "mykey", "value": 42}'
```

## Run Tests

```bash
# Run the test script
./scripts/test-cluster.sh
```

## Verify Consensus

After setting values via one node, verify they appear on all nodes:

```bash
# Set via node 0
curl -X POST http://localhost:8080/kv/test -d '{"value": 42}'

# Wait for consensus
sleep 2

# Check on all nodes
for port in 8080 8081 8082 8083 8084; do
  echo "Node $port:"
  curl -s "http://localhost:$port/kv/test" | jq .
done
```

## Stop the Cluster

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean state)
docker-compose down -v
```

## Troubleshooting

### Check if TikV is running
```bash
docker-compose ps pd0 tikv0
```

### View TikV logs
```bash
docker-compose logs pd0 tikv0
```

### Restart a specific node
```bash
docker-compose restart kvnode0
```

### Connect to a container
```bash
docker-compose exec kvnode0 /bin/sh
```
