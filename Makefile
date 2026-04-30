# Project Variables
BAKE_FILE := deployments/docker-bake.hcl
COMPOSE_FILE := deployments/docker-compose.yml
PROJECT_NAME := od
TAG ?= latest

# BuildKit entitlement suppression
export BUILDX_BAKE_ENTITLEMENTS_FS=0

COMPOSE := docker compose -p $(PROJECT_NAME) -f $(COMPOSE_FILE) --profile all
BAKE := docker buildx bake -f $(BAKE_FILE)
.PHONY: help clean build rebuild up upd down downv restart restartd logs ps metrics \
        build-svc logs-svc shell-db shell-redis prepare-proxy armageddon

help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

clean: ## Remove BuildKit build cache
	docker buildx prune -af

build: ## Build all service images locally using cache
	$(BAKE) --load

rebuild: ## Force-rebuild all images locally
	$(BAKE) --load --no-cache

up: ## Start the full pipeline in foreground
	$(COMPOSE) up

upd: ## Start the full pipeline detached
	$(COMPOSE) up -d

down: ## Stop and remove containers
	$(COMPOSE) down

downv: ## Stop and remove containers and volumes
	$(COMPOSE) down -v

restart: down up ## Restart the full pipeline in foreground

restartd: down upd ## Restart the full pipeline detached

logs: ## Follow all logs
	$(COMPOSE) logs -f

ps: ## View service status
	$(COMPOSE) ps

metrics: ## Curl the monitor service metrics
	@curl -s http://localhost:8080/metrics | jq .

build-svc: ## Build one service: make build-svc SVC=parser
	@test -n "$(SVC)" || (echo "Usage: make build-svc SVC=parser" && exit 1)
	$(BAKE) --load $(SVC)

logs-svc: ## Follow one service logs: make logs-svc SVC=parser
	@test -n "$(SVC)" || (echo "Usage: make logs-svc SVC=parser" && exit 1)
	$(COMPOSE) logs -f $(SVC)

shell-db: ## Open psql shell
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-postgres} -d $${POSTGRES_DB:-pipeline}

shell-redis: ## Open redis-cli shell
	$(COMPOSE) exec redis redis-cli

armageddon: ## Remove all Docker containers, networks, volumes, images, and cache
	@echo "--- Nuking all Docker resources ---"
	@docker ps -aq | xargs -r docker stop
	@docker ps -aq | xargs -r docker rm -f
	@docker network prune -f
	@docker volume prune -f
	@docker image prune -af
	@docker builder prune -af
	@echo "--- Armageddon complete ---"