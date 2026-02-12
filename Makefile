# Load .env file if it exists
ifneq (,$(wildcard ./.env))
    include .env
    export
endif

# Fallback defaults if .env is missing
DB_CONTAINER=telescope-postgres
DB_USER ?= user
DB_NAME ?= telescope
MIGRATION_FILE=.\internal\migrations\000001_init_schema.up.sql
PROTO_FILE=./proto/telescope.proto
PROTO_DIR=proto

.PHONY: help up down build migrate producer logs psql clean proto

# Default target: Show help
help:
	@echo "🔭 Telescope Make Commands:"
	@echo "  make up        - Start Zookeeper, Kafka, Postgres, and Storage Node"
	@echo "  make down      - Stop all containers"
	@echo "  make build     - Rebuild Docker images (Storage & Producer)"
	@echo "  make migrate   - Apply the database schema (Run after 'make up')"
	@echo "  make producer  - Fire the test producer (Send logs to Kafka)"
	@echo "  make logs      - Tail the logs of the Storage Node"
	@echo "  make psql      - Connect to the Postgres database shell"
	@echo "  make clean     - Stop containers and remove volumes (Full Reset)"

up:
	@echo "🚀 Starting Telescope Infrastructure..."
	docker-compose up -d zookeeper kafka postgres storage search-api

down:
	@echo "🛑 Stopping Telescope..."
	docker-compose down

build:
	@echo "🔨 Building Images..."
	docker-compose build --no-cache

# 2. Database Management 💾
migrate:
	@echo "📦 Applying Migrations..."
	type $(MIGRATION_FILE) | docker exec -i $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)

psql:
	@echo "🐚 Entering Postgres Shell..."
	docker exec -it $(DB_CONTAINER) psql -U $(DB_USER) -d $(DB_NAME)

# 3. Traffic Generation 🔫
producer:
	@echo "🔥 Firing Producer..."
	docker-compose run --rm producer
search:
	@echo "Starting Search Service"
	docker-compose run --rm search-api

# 4. Observability 👀
logs:
	@echo "📜 Tailing Storage Node Logs..."
	docker-compose logs -f storage search-api

# 5. Nuclear Option ☢️
clean:
	@echo "🧹 Cleaning up everything (including data)..."
	docker-compose down -v
proto:
	@echo "🤖 Generating gRPC code..."
	"C:\Tools\protoc\bin\protoc.exe" --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative $(PROTO_FILE)