build:
  docker build -t manifoldlabs/targon-modelo .

run: build
  docker run --env-file .env -d --name targon-modelo manifoldlabs/targon-modelo

# Alias for the run command
up: run
