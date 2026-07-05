# meili-mysql-sync

Outil Rust de synchronisation MySQL -> Meilisearch.

Il fait deux choses :

1. un snapshot initial par pagination sur la cle primaire, sans charger toute la table en memoire ;
2. une ecoute row-based du binlog MySQL pour rejouer les inserts, updates et deletes en temps reel.

## Prerequis MySQL

MySQL doit etre configure en row-based binlog :

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

Valeurs attendues :

```text
log_bin = ON
binlog_format = ROW
binlog_row_image = FULL
```

`binlog_row_image=MINIMAL` fonctionne pour filtrer les updates, mais l'outil relira la ligne dans MySQL quand un update ne contient pas tous les champs du document.
Si `where_clause` depend de colonnes non presentes dans `fields`, ajoute ces colonnes a `watch_fields` pour que leurs updates declenchent une relecture et, si necessaire, une suppression dans Meilisearch.

L'utilisateur MySQL a besoin des droits de lecture sur les tables, `information_schema`, et du flux replication :

```sql
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'sync_user'@'%';
```

## Utilisation

Copier `config.example.toml` vers `config.toml`, adapter les tables, puis lancer :

```bash
cargo run --release -- run
```

Commandes utiles :

```bash
cargo run --release -- position
cargo run --release -- snapshot
cargo run --release -- snapshot --recreate-indexes
cargo run --release -- snapshot --clear-documents
cargo run --release -- cdc --file mysql-bin.000001 --pos 4
```

## Docker

Construire l'image :

```powershell
docker build -t meili-mysql-sync:local .
```

Le conteneur attend la configuration dans `/config/config.toml` et ecrit l'etat
dans `/data` quand `runtime.state_path` est relatif, comme dans
`config.example.toml`.

Exemple de lancement en arriere-plan depuis PowerShell :

```powershell
docker volume create meili-sync-data

docker run -d `
  --name meili-mysql-sync `
  --restart unless-stopped `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local
```

Si MySQL et Meilisearch tournent dans d'autres conteneurs, ajoute
`--network <nom-du-reseau-docker>` et utilise leurs noms de services dans
`config.toml`.

Le conteneur lance `run` par defaut. Pour executer une autre commande :

```powershell
docker run --rm `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local position
```

```powershell
docker run --rm `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local snapshot --recreate-indexes
```

Commandes d'exploitation :

```powershell
docker logs -f meili-mysql-sync
docker stop meili-mysql-sync
docker rm meili-mysql-sync
```

Variables d'environnement utiles :

| Variable | Obligatoire | Defaut | Description |
| --- | --- | --- | --- |
| `RUST_LOG` | Non | `info` | Niveau de logs Rust/tracing. Exemples : `debug`, `info`, `warn`, `meili_mysql_sync=debug`. |

Les autres parametres ne sont pas lus depuis l'environnement par l'application :
ils doivent rester dans `config.toml`.

Parametres importants a verifier dans `config.toml` pour Docker :

| Chemin TOML | Exemple Docker | Description |
| --- | --- | --- |
| `mysql.url` | `mysql://sync_user:sync_password@mysql:3306/football_data` | Utiliser le nom du service Docker MySQL, ou `host.docker.internal` si MySQL tourne sur la machine hote. |
| `mysql.server_id` | `7101` | Identifiant de replication unique pour ce client CDC. |
| `meilisearch.host` | `http://meilisearch:7700` | Utiliser le nom du service Docker Meilisearch, ou `host.docker.internal` si Meilisearch tourne sur la machine hote. |
| `meilisearch.api_key` | `masterKey` | Cle API Meilisearch. Ne pas la mettre dans l'image Docker. |
| `runtime.state_path` | `meili-sync-state.json` ou `/data/meili-sync-state.json` | Fichier de checkpoint binlog. Il doit etre conserve sur un volume persistant. |

Dans un conteneur, `127.0.0.1` designe le conteneur lui-meme. Ne l'utilise
dans `mysql.url` ou `meilisearch.host` que si le service correspondant tourne
dans le meme conteneur.

### Reutiliser l'image dans un autre projet ou en prod

Oui, l'image Docker est generique : pour la reutiliser ailleurs, tu n'as pas
besoin de reconstruire l'image si le binaire convient. Tu dois fournir :

1. une image disponible sur la machine ou dans un registry ;
2. un fichier `config.toml` adapte au projet ;
3. un volume persistant pour `/data` ;
4. un acces reseau depuis le conteneur vers MySQL et Meilisearch.

Procedure exacte :

1. Publier ou recuperer l'image.

   Depuis ce depot :

   ```bash
   docker build -t registry.example.com/meili-mysql-sync:0.1.0 .
   docker push registry.example.com/meili-mysql-sync:0.1.0
   ```

   Sur le serveur de prod :

   ```bash
   docker pull registry.example.com/meili-mysql-sync:0.1.0
   ```

2. Creer un `config.toml` dans le projet ou sur le serveur.

   Exemple minimal à adapter :

   ```toml
   [mysql]
   url = "mysql://sync_user:sync_password@mysql:3306/football_data"
   server_id = 7101

   [meilisearch]
   host = "http://meilisearch:7700"
   api_key = "masterKey"
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

   Points a verifier :

   - `mysql.url` doit etre joignable depuis le conteneur ;
   - `meilisearch.host` doit etre joignable depuis le conteneur ;
   - `mysql.server_id` doit etre unique pour chaque instance de sync ;
   - `runtime.state_path` doit pointer vers `/data/...` pour garder le
     checkpoint dans le volume ;
   - la cle Meilisearch reste dans `config.toml` ou dans un fichier secret
     monte comme config, jamais dans l'image.

3. Creer le volume d'etat.

   ```bash
   docker volume create meili-sync-data
   ```

4. Lancer le conteneur.

   Si MySQL et Meilisearch sont sur le meme reseau Docker :

   ```bash
   docker run -d \
     --name meili-mysql-sync \
     --restart unless-stopped \
     --network app-network \
     --env RUST_LOG=info \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=volume,source=meili-sync-data,target=/data \
     registry.example.com/meili-mysql-sync:0.1.0
   ```

   Si MySQL et Meilisearch tournent sur la machine hote :

   ```bash
   docker run -d \
     --name meili-mysql-sync \
     --restart unless-stopped \
     --add-host=host.docker.internal:host-gateway \
     --env RUST_LOG=info \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=volume,source=meili-sync-data,target=/data \
     registry.example.com/meili-mysql-sync:0.1.0
   ```

   Dans ce deuxieme cas, utilise `host.docker.internal` dans `mysql.url` et
   `meilisearch.host`.

5. Verifier le demarrage.

   ```bash
   docker ps
   docker logs -f meili-mysql-sync
   ```

   Le demarrage attendu est :

   - snapshot initial si aucun checkpoint n'existe ;
   - creation ou mise a jour des index Meilisearch ;
   - ecriture du checkpoint dans `/data` ;
   - passage en ecoute CDC du binlog MySQL.

6. Mettre a jour l'image en prod.

   Ne supprime pas le volume `meili-sync-data`, sinon le conteneur perd la
   position binlog sauvegardee.

   ```bash
   docker pull registry.example.com/meili-mysql-sync:0.1.1
   docker stop meili-mysql-sync
   docker rm meili-mysql-sync
   docker run -d \
     --name meili-mysql-sync \
     --restart unless-stopped \
     --network app-network \
     --env RUST_LOG=info \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=volume,source=meili-sync-data,target=/data \
     registry.example.com/meili-mysql-sync:0.1.1
   ```

Exemple `docker-compose.yml` pour une reutilisation plus simple :

```yaml
services:
  meili-mysql-sync:
    image: registry.example.com/meili-mysql-sync:0.1.0
    container_name: meili-mysql-sync
    restart: unless-stopped
    environment:
      RUST_LOG: info
    volumes:
      - ./config.toml:/config/config.toml:ro
      - meili-sync-data:/data
    networks:
      - app-network

volumes:
  meili-sync-data:

networks:
  app-network:
    external: true
```

Avec Compose :

```bash
docker compose up -d
docker compose logs -f meili-mysql-sync
```

`snapshot` cree l'index Meilisearch s'il est absent, applique les reglages
declares dans `config.toml`, puis envoie les documents par lots. Les options
suivantes servent aux reconstructions completes :

- `--recreate-indexes` supprime puis recree les index avant le snapshot ;
- `--clear-documents` garde les reglages de l'index mais supprime les documents
  avant de les recharger.

Pour accelerer l'indexation, declare explicitement les champs
`searchable_attributes`, `filterable_attributes` et `sortable_attributes`.
Meilisearch indexe tous les champs texte par defaut ; limiter
`searchable_attributes` aux champs vraiment recherches reduit fortement le
travail pendant la creation de l'index.

Le fichier `state_path` contient la position binlog checkpointee apres flush et validation des taches Meilisearch.
