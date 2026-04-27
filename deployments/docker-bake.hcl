variable "TAG" {
  default = "latest"
}

group "default" {
  targets = ["collector", "fetcher", "parser", "monitor", "proxy-manager", "enricher", "partition-manager"]
}

# 1. The base image
target "base" {
  context    = "."
  dockerfile = "deployments/Dockerfile.base"
  tags       = ["od-base:${TAG}"]
}

# 2. The common configuration
target "_common" {
  context  = "."
  depends_on = ["base"]
  contexts = {
    "od-base-local" = "target:base"
  }
}

# 3. Service targets
target "collector" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.collector"
  tags       = ["deployments-collector:${TAG}"]
}

target "fetcher" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.fetcher"
  tags       = ["deployments-fetcher:${TAG}"]
}

target "parser" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.parser"
  tags       = ["deployments-parser:${TAG}"]
}

target "monitor" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.monitor"
  tags       = ["deployments-monitor:${TAG}"]
}

target "proxy-manager" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.proxy-manager"
  tags       = ["deployments-proxy-manager:${TAG}"]
}

target "enricher" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.enricher"
  tags       = ["deployments-enricher:${TAG}"]
}

target "partition-manager" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.partition-manager"
  tags       = ["deployments-partition-manager:${TAG}"]
}