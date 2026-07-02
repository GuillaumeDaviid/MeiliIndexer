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
