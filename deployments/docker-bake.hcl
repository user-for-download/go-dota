group "default" {
  targets = ["collector", "fetcher", "parser", "monitor", "proxy-manager"]
}

# Shared base: deps + source, built once and reused by all services
target "base" {
  context    = ".."
  dockerfile = "deployments/Dockerfile.base"
  tags       = ["od-base:latest"]
}

target "_common" {
  context  = ".."
  contexts = {
    "od-base:latest" = "target:base"
  }
}

target "collector" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.collector"
  tags       = ["deployments-collector:latest"]
}

target "fetcher" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.fetcher"
  tags       = ["deployments-fetcher:latest"]
}

target "parser" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.parser"
  tags       = ["deployments-parser:latest"]
}

target "monitor" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.monitor"
  tags       = ["deployments-monitor:latest"]
}

target "proxy-manager" {
  inherits   = ["_common"]
  dockerfile = "deployments/Dockerfile.proxy-manager"
  tags       = ["deployments-proxy-manager:latest"]
}