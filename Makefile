# Project Variables
BAKE_FILE := deployments/docker-bake.hcl
COMPOSE_FILE := deployments/docker-compose.yml
PROJECT_NAME := od

# BuildKit entitlement suppression
export BUILDX_BAKE_ENTITLEMENTS_FS=0

.PHONY: help clean build rebuild up down restart restartd logs ps metrics build-svc logs-svc shell-db shell-redis

clean: ## Remove build cache and base images
	docker buildx prune -af

build: ## Build all service images (cached)
	docker buildx bake -f $(BAKE_FILE)

rebuild: ## Force-rebuild all images
	docker buildx bake -f $(BAKE_FILE) --no-cache

up: ## Start the full pipeline (foreground)
	docker compose -f $(COMPOSE_FILE)  --profile all up

upd: ## Start the full pipeline (detached)
	docker compose -f $(COMPOSE_FILE)  --profile all up -d

down: ## Stop and remove containers
	docker compose -f $(COMPOSE_FILE)  --profile all down

downv: ## Stop and remove containers and volumes
	docker compose -f $(COMPOSE_FILE)  --profile all down -v

logs: ## Follow logs
	docker compose -f $(COMPOSE_FILE) --profile all logs -f

ps: ## View running service status
	docker compose -f $(COMPOSE_FILE)  ps

metrics: ## Curl the monitor service metrics
	@curl -s http://localhost:8080/metrics | jq .

shell-db: ## Open psql shell
	docker compose -f $(COMPOSE_FILE) exec postgres psql -U postgres -d pipeline

shell-redis: ## Open redis-cli shell
	docker compose -f $(COMPOSE_FILE) exec redis redis-cli

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