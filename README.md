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

L'utilisateur MySQL a besoin des droits de lecture sur les seules bases synchronisees et du flux
de replication. Ne lui accorde pas `SELECT` sur toutes les bases :

```sql
GRANT SELECT ON football_data.* TO 'sync_user'@'%';
GRANT REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'sync_user'@'%';
```

## Utilisation

Copier `config.example.toml` vers `config.toml`, adapter les tables, puis lancer :

```bash
cargo run --release -- run
```

Les secrets peuvent remplacer les valeurs TOML sans etre stockes dans `config.toml` :

```bash
export MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url
export MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key
```

Chaque fichier contient uniquement le secret correspondant. Les variantes directes
`MEILI_SYNC_MYSQL_URL` et `MEILI_SYNC_MEILISEARCH_API_KEY` existent pour le developpement,
mais les variantes `_FILE` sont preferees en production.

Hors `localhost`, `127.0.0.0/8` et `::1`, MySQL doit utiliser `require_ssl=true` et
Meilisearch doit utiliser `https://`. `allow_insecure = true` existe dans chaque section
uniquement pour une derogation reseau explicite.

Commandes utiles :

```bash
cargo run --release -- position
cargo run --release -- snapshot
cargo run --release -- snapshot --recreate-indexes
cargo run --release -- snapshot --clear-documents
cargo run --release -- cdc --file mysql-bin.000001 --pos 4
```

### Metriques de performance

Ajouter `--metrics` pour produire, a la fin de la synchronisation, deux logs
`INFO` structures : un bilan des volumes et ressources, puis la repartition du
temps par etape. L'option est disponible pour `run`, `snapshot` et `cdc` et
reste desactivee par defaut.

```bash
cargo run --release -- snapshot --metrics
cargo run --release -- run --metrics
cargo run --release -- cdc --metrics --file mysql-bin.000001 --pos 4
```

`snapshot` s'arrete apres le chargement et ecrit immediatement son bilan.
`run` et `cdc` ecrivent aussi un bilan cumule a chaque intervalle de flush qui
a recu des evenements CDC, puis un bilan final lors d'un arret propre (par
exemple `Ctrl+C`) ou si la synchronisation se termine sur une erreur.
Les valeurs des documents et leurs identifiants ne sont jamais ecrits dans les logs.

Exemple de sortie (les champs sont des paires `cle=valeur` exploitables par un
collecteur de logs) :

```text
INFO synchronisation terminee sync_run_id="..." sync_mode="full" duration_ms=48231 documents_read=125000 documents_indexed=124998 documents_failed=2 documents_per_second=2591.7 bytes_read=184293891 bytes_sent=91293821 average_batch_size=487.0 peak_memory_mb=184 average_cpu_percent=72.4 rss_bytes=192937984 virtual_memory_bytes=823132160 peak_rss_bytes=192937984
INFO repartition du temps de synchronisation sync_run_id="..." sync_duration_ms=48231 mysql_read_ms=12843 transformation_ms=4382 serialization_ms=2144 batch_wait_ms=318 meilisearch_http_ms=11622 meilisearch_task_wait_ms=16417 checkpoint_write_ms=42
```

`bytes_read` est la taille du JSON normalise des documents lus depuis MySQL,
et `bytes_sent` celle des lots JSON envoyes a Meilisearch. `rss_bytes`,
`virtual_memory_bytes` et `peak_rss_bytes` concernent le processus de
synchronisation; les ressources sont echantillonnees toutes les 500 ms. Les
compteurs de documents indexes et en echec sont mis a jour lorsque les taches
Meilisearch sont validees ou echouent.

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

Pour obtenir le bilan de performance dans les logs du conteneur :

```powershell
docker run --rm `
  --env RUST_LOG=info `
  --mount type=bind,source="${PWD}\config.toml",target=/config/config.toml,readonly `
  --mount type=volume,source=meili-sync-data,target=/data `
  meili-mysql-sync:local snapshot --metrics
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
| `MEILI_SYNC_MYSQL_URL_FILE` | Production | - | Fichier contenant l'URL MySQL complete, mot de passe inclus. |
| `MEILI_SYNC_MEILISEARCH_API_KEY_FILE` | Production | - | Fichier contenant une cle Meilisearch restreinte. |
| `MEILI_SYNC_MYSQL_URL` | Non | - | Remplacement direct de `mysql.url`, moins adapte aux secrets de production. |
| `MEILI_SYNC_MEILISEARCH_API_KEY` | Non | - | Remplacement direct de `meilisearch.api_key`. |

Ne definis pas simultanement une variable directe et sa variante `_FILE`.

Parametres importants a verifier dans `config.toml` pour Docker :

| Chemin TOML | Exemple Docker | Description |
| --- | --- | --- |
| `mysql.url` | `mysql://sync_user@mysql:3306/football_data?require_ssl=true` | Utiliser TLS hors loopback et fournir l'URL avec secret via la variante `_FILE`. |
| `mysql.server_id` | `7101` | Identifiant de replication unique pour ce client CDC. |
| `meilisearch.host` | `https://meilisearch:7700` | Utiliser HTTPS hors loopback. |
| `meilisearch.api_key` | omis | Fournir une cle limitee aux index et actions necessaires via la variante `_FILE`. |
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
   # Remplace par MEILI_SYNC_MYSQL_URL_FILE au demarrage.
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

   Points a verifier :

   - l'URL MySQL fournie par secret doit contenir `require_ssl=true` ;
   - `meilisearch.host` doit etre joignable en HTTPS depuis le conteneur ;
   - `mysql.server_id` doit etre unique pour chaque instance de sync ;
   - `runtime.state_path` doit pointer vers `/data/...` pour garder le
     checkpoint dans le volume ;
   - la cle Meilisearch doit etre restreinte aux index et actions necessaires et montee
     dans un fichier secret, jamais placee dans l'image ou `config.toml`.

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
     --env MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url \
     --env MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=bind,source="$(pwd)/mysql-url.secret",target=/run/secrets/mysql-url,readonly \
     --mount type=bind,source="$(pwd)/meilisearch-api-key.secret",target=/run/secrets/meilisearch-api-key,readonly \
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
     --env MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url \
     --env MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=bind,source="$(pwd)/mysql-url.secret",target=/run/secrets/mysql-url,readonly \
     --mount type=bind,source="$(pwd)/meilisearch-api-key.secret",target=/run/secrets/meilisearch-api-key,readonly \
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
     --env MEILI_SYNC_MYSQL_URL_FILE=/run/secrets/mysql-url \
     --env MEILI_SYNC_MEILISEARCH_API_KEY_FILE=/run/secrets/meilisearch-api-key \
     --mount type=bind,source="$(pwd)/config.toml",target=/config/config.toml,readonly \
     --mount type=bind,source="$(pwd)/mysql-url.secret",target=/run/secrets/mysql-url,readonly \
     --mount type=bind,source="$(pwd)/meilisearch-api-key.secret",target=/run/secrets/meilisearch-api-key,readonly \
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
