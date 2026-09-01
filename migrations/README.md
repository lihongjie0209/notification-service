# Migrations

Database-specific migrations live under `mysql`, `postgres`, and `kingbase`. Set `migration.path` to the matching directory, for example `migrations/postgres`. Review indexes, collation and online-DDL impact against production data before deployment.

Application scoping is deployed in two stages. Migration `000005` adds nullable scope columns and indexes. Before applying `000006` to a database that already contains notifications, backfill every template and delivery with its authoritative application ID; the contract migration intentionally fails while null or empty scopes remain.
