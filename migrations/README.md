# Migration design notes

Applied SQLx migrations are immutable after publication because SQLx records
their checksums. The initial coordination schema is `0001_jobs.sql`; encrypted
WeRead account persistence is added by the forward `0002_weread_accounts.sql`
migration. This project has not published a release yet, so either file may
still be revised before the first release if its contract changes.

The forward `0003_optional_source_article_url.sql` migration allows a source
to be created from a known WeRead `book_id` without inventing a public article
URL. Existing URLs remain unchanged and new URLs are still validated by the
domain before persistence.

The forward `0004_asset_cache.sql` migration adds PostgreSQL-backed asset
metadata, deduplicated binary blobs, and article-to-asset relationships. Binary
eviction clears only blob data and retains referenced URL/version metadata for
repair workers; orphan cleanup removes rows that no longer belong to an article.

The forward `0005_asset_repair.sql` migration adds the `asset_repair` job kind,
durable per-asset admission/backoff state, and job-to-asset links used to
deduplicate public cache-miss repair work and enforce cluster-wide limits.

The initial schema provides:

- the active non-failure `deferred` job state;
- separate durable `claim_count` and retry-budget `failure_count` values;
- active deduplication and claim indexes that include `deferred`; and
- constraints that prevent claims from consuming the failure budget;
- source identity/configuration, scheduling state, gates, cooldowns,
  reservations, and feed revision used to fence feed publication and
  atomically enqueue due work;
- an optional stable `account_id` relationship for authenticated list
  acquisition;
- encrypted WeRead account credential metadata and optimistic credential
  versions in `weread_accounts`; raw access and refresh tokens are never
  stored in this table;
- revision-aware `feed_cache` rows with a source foreign key;
- hash-only `feed_tokens` rows with one rotatable/revocable public capability
  per source;
- fenced `account_leases` for authenticated-account serialization;
- fenced `feed_build_leases` for per-source cache-build single-flight;
- normalized `articles` keyed by `(source_id, review_id)`, including sanitized
  HTML, optional external URLs, the pre-acquisition observation version, and
  the feed-order index; and
- `asset_blobs`, `asset_records`, and `article_assets` for the optional
  database asset cache, including checksum/raw-byte deduplication and
  stable metadata after binary eviction; and
- `sync_runs` audit rows with typed outcomes, bounded counters, safe failure
  summaries, and optional published feed revisions.

There is no legacy `attempts` column or compatibility trigger. When a release
has been published, later changes must use a new forward migration and must not
edit `0001_jobs.sql`.

Later forward migrations are added only as their executable repository contract
is implemented. They may:

- add later source lifecycle fields;
- add later credential/login fields or archive tables; and
- extend the feed-cache rows only if the final RSS contract needs fields beyond
  the current XML/ETag/revision payload.

Every migration is covered by PostgreSQL upgrade tests as well as clean-database
`#[sqlx::test]` tests.
