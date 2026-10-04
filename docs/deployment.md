# Docker deployment

The image is reusable across projects: supply a project-specific
configuration, secret files, persistent state storage, and network access to
MySQL and Meilisearch.

## Build and distribute the image

```bash
docker build -t meili-mysql-sync:local .
```

To publish a version to your registry, replace the registry and version below:

```bash
docker build -t registry.example.com/meili-mysql-sync:0.1.0 .
docker push registry.example.com/meili-mysql-sync:0.1.0
```

On the deployment host:

```bash
docker pull registry.example.com/meili-mysql-sync:0.1.0
```

## Configure the service

Mount the configuration at `/config/config.toml`. The working directory is
`/data`, so relative state paths are written there. Use a persistent volume
for `/data` and keep it across upgrades.

Example production configuration:

```toml
[mysql]
# Supply the full connection URL through MEILI_SYNC_MYSQL_URL_FILE.
url = ""
server_id = 7101

[meilisearch]
host = "https://meilisearch:7700"
batch_size = 50000
max_in_flight_tasks = 4
task_poll_ms = 100
task_timeout_secs = 600

[runtime]
state_path = "/data/meili-sync-state.json"
snapshot_on_start = true
flush_interval_ms = 1000
checkpoint_every_events = 1000

[[tables]]
database = "football_data"
table = "clubs"
index = "clubs"
primary_key = "club_id"
fields = ["club_id", "name"]
searchable_attributes = ["name"]
snapshot_batch_size = 50000
```

Create `mysql-url.secret` containing the full connection URL, for example
`mysql://sync_user:password@mysql:3306/football_data?require_ssl=true`, and
`meilisearch-api-key.secret` containing an API key restricted to the required
indexes and actions. Keep secrets out of the image and configuration file.
Mounted configuration and secret files must be readable by the container's
nonroot user. See [configuration](configuration.md) for all options.

## Networking

If the dependencies run in other containers, attach the synchronization
container to their Docker network and use service names such as `mysql` and
`meilisearch` in the configuration. The examples below assume an existing
`app-network`.

Inside a container, `127.0.0.1` refers to that container. To reach services
on the host, use `host.docker.internal` in the MySQL URL and Meilisearch host.
On Linux, add `--add-host=host.docker.internal:host-gateway` to `docker run`.

Non-loopback endpoints require MySQL TLS (`require_ssl=true`) and Meilisearch
HTTPS. Private Docker networks also require these settings unless you
explicitly set `allow_insecure = true` in the corresponding service section.

## Start the container

Create the persistent state volume:

```bash
docker volume create meili-sync-data
```

Run from the directory containing the configuration and secret files (Bash):

```bash
docker run -d \
  --name meili-mysql-sync \
  --restart unless-stopped \
  --network app-network \
  --env RUST_LOG=info \
  --env MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url \
  --env MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key \
  --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
  --mount type=bind,source="$(pwd)/mysql-url.secret",target=/run/secrets/mysql-url,readonly \
  --mount type=bind,source="$(pwd)/meilisearch-api-key.secret",target=/run/secrets/meilisearch-api-key,readonly \
  --mount type=volume,source=meili-sync-data,target=/data \
  registry.example.com/meili-mysql-sync:0.1.0
```

The container runs `run` by default. On the first startup with automatic
snapshots enabled, it prepares the indexes, loads the snapshot, saves the
binlog checkpoint, and starts CDC. Later startups resume from the checkpoint.

Check and manage the container:

```bash
docker ps
docker logs -f meili-mysql-sync
docker stop meili-mysql-sync
docker rm meili-mysql-sync
```

## Run another command

Append a command after the image name. Reuse the same configuration, secrets,
network, and state mounts. For example, in PowerShell with credentials already
provided by the configuration or environment:

```powershell
docker run --rm `
  --network app-network `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local position
```

Replace `position` with `snapshot --recreate-indexes` to rebuild indexes or
`snapshot --metrics` to report snapshot performance. For secret-file
configuration, include the secret environment variables and mounts shown in
the production command.

## Docker Compose

Save this as `docker-compose.yml` next to the configuration and secret files:

```yaml
services:
  meili-mysql-sync:
    image: registry.example.com/meili-mysql-sync:0.1.0
    container_name: meili-mysql-sync
    restart: unless-stopped
    environment:
      RUST_LOG: info
      MEILI_SYNC_MYSQL_URL_FILE: /run/secrets/mysql-url
      MEILI_SYNC_MEILISEARCH_API_KEY_FILE: /run/secrets/meilisearch-api-key
    volumes:
      - ./config.toml:/config/config.toml:ro
      - ./mysql-url.secret:/run/secrets/mysql-url:ro
      - ./meilisearch-api-key.secret:/run/secrets/meilisearch-api-key:ro
      - meili-sync-data:/data
    networks:
      - app-network

volumes:
  meili-sync-data:

networks:
  app-network:
    external: true
```

```bash
docker compose up -d
docker compose logs -f meili-mysql-sync
```

## Upgrade

For Compose, change the image tag in `docker-compose.yml`, then run:

```bash
docker compose pull
docker compose up -d
```

For `docker run`, pull the new version, stop and remove the old container,
then rerun the startup command with the new tag and the same mounts:

```bash
docker pull registry.example.com/meili-mysql-sync:0.1.1
docker stop meili-mysql-sync
docker rm meili-mysql-sync
```

Keep `meili-sync-data`; deleting it loses the saved binlog position.
