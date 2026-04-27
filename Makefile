# Project Variables
BAKE_FILE := deployments/docker-bake.hcl
COMPOSE_FILE := deployments/docker-compose.yml
PROJECT_NAME := od

# BuildKit entitlement suppression
export BUILDX_BAKE_ENTITLEMENTS_FS=0

.PHONY: help clean build rebuild up down restart restartd logs ps metrics build-svc logs-svc shell-db shell-redis

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

clean: ## Remove build cache and base images
	docker buildx prune -af
	-docker image rm -f od-base:latest \
		deployments-collector:latest deployments-fetcher:latest \
		deployments-parser:latest deployments-monitor:latest \
		deployments-proxy-manager:latest deployments-enricher:latest \
		deployments-partition-manager:latest || true

build: ## Build all service images (cached)
	docker buildx bake -f $(BAKE_FILE)

rebuild: ## Force-rebuild all images
	docker buildx bake -f $(BAKE_FILE) --no-cache

up-init:
	docker compose -f $(COMPOSE_FILE) --profile init --profile db up

down-init:
	docker compose -f $(COMPOSE_FILE) --profile init --profile db down -v

up: ## Start the full pipeline (foreground)
	docker compose -f $(COMPOSE_FILE)  --profile all up

upd: ## Start the full pipeline (detached)
	docker compose -f $(COMPOSE_FILE)  --profile all up -d

down: ## Stop and remove containers
	docker compose -f $(COMPOSE_FILE)  --profile all down

downv: ## Stop and remove containers and volumes
	docker compose -f $(COMPOSE_FILE)  --profile all down -v

fetch:
	docker compose -f deployments/docker-compose.yml \
	  --profile db --profile fetch \
	  run --rm fetcher --key=default

restart: down upd ## Restart the pipeline (detached)

logs: ## Follow logs
	docker compose -f $(COMPOSE_FILE) --profile all logs -f

ps: ## View running service status
	docker compose -f $(COMPOSE_FILE)  ps

metrics: ## Curl the monitor service metrics
	@curl -s http://localhost:8080/metrics | jq .

build-svc: ## Build a single service: make build-svc SVC=collector
	docker buildx bake -f $(BAKE_FILE) base $(SVC)

logs-svc: ## Tail one service: make logs-svc SVC=parser
	docker compose -f $(COMPOSE_FILE) -p  logs -f $(SVC)

shell-db: ## Open psql shell
	docker compose -f $(COMPOSE_FILE) -p  exec postgres psql -U postgres -d pipeline

shell-redis: ## Open redis-cli shell
	docker compose -f $(COMPOSE_FILE) -p  exec redis redis-cli

.PHONY: armageddon
armageddon:
	@echo "--- Nuking all Docker resources ---"
	# Stop and remove all containers
	@docker ps -aq | xargs -r docker stop
	@docker ps -aq | xargs -r docker rm -f
	# Remove all networks (except defaults)
	@docker network prune -f
	# Remove all volumes
	@docker volume prune -f
	# Remove dangling images
	@docker image prune -f
	# Force remove all images
	@docker images -qa | xargs -r docker rmi -f
	@echo "--- Armageddon complete ---"