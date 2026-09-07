# Werrss Rust Architecture

This document describes the planned Rust implementation of the existing
`werrss-main` Python service. The current Rust tree remains intentionally
incremental: it defines boundaries and ownership, with the domain/configuration
policies and the first PostgreSQL job/cache-persistence slices implemented.
Administrative HTTP behavior
for the single configured administrator is executable.
Process liveness and PostgreSQL-backed API readiness diagnostics are executable.
Encrypted credential persistence and non-interactive
refresh are implemented as an application service.
The pure RSS renderer, normalized article persistence, cache-first feed
delivery decision service, archive sanitizer, unauthenticated public article
browser path, and database-only feed rebuild orchestration are executable.
The public feed-token lifecycle and its tokenized feed route are executable;
administrative routes compose the source, feed-token, and synchronization-run
application/repository boundaries. The first usable version also includes a
small web UI over those application/API boundaries. The panel supports English,
French, and Simplified Chinese through a browser preference cookie and
`Accept-Language` negotiation, with English as the fallback.

The first usable version includes interactive QR-code login and a durable
article-backfill queue. Source synchronization records fetchable per-article
failures as deduplicated repair jobs; a worker retries each job independently,
and a successful repair publishes a new feed revision and follow-up rebuild.

## Goals

- Fetch WeChat public-account articles through a browser-driven acquisition
  layer.
- Archive sanitized article HTML; article-asset caching is optional in version
  one.
- Generate RSS feeds from archived data.
- Support multiple application instances with PostgreSQL-coordinated jobs.
- Keep RSS requests fast through a persisted 30-minute feed cache.
- Apply configurable, bounded pacing between upstream requests and page
  operations.
- Pause new upstream work during configured quiet hours in an IANA timezone.
- Keep authentication, browser protocol details, business rules, and storage
  replaceable independently.
- Provide a small authenticated admin panel for source management,
  synchronization status, feed-link copying, and safe error presentation. The
  first version has one administrator configured by environment; user
  management is out of scope.

## Implementation status and contract policy

This document describes the target version-one architecture and the behavior
of the currently executable runtime. The binary now starts the process
supervisor from the side-effect-free `RuntimePlan`; the
foundations are typed environment parsing for the target configuration set,
pure pacing/quiet-hours policy, PostgreSQL pool/migration helpers, the job and
feed-cache domain/repository slices, their shared transaction boundary, the
stable account identity plus distributed account-lease slice, the per-source
feed-build lease slice, atomic due-source scheduling persistence, and the
hash-only public feed-token lifecycle, the unauthenticated Thirtyfour
public-page identity/navigation/extraction, bounded pacing/scroll, and expected
browser-timezone validation slice.

The current and target contracts must not be confused:

| Area | Executable now | Target contract and implementation gate |
| --- | --- | --- |
| Runtime | `RuntimeSupervisor` consumes the validated `RuntimePlan`, opens the shared PostgreSQL pool, applies SQLx migrations, binds the selected API, and supervises scheduler, feed-rebuild, and account-selection-at-job-time source-sync loops with graceful shutdown; an injected refresh transport also enables account-expiry scheduling; a shared browser-health monitor gates browser-backed claims | QR/login exchange uses the admin router's bounded process-local manager; API liveness/readiness, browser-worker readiness, and single-admin routes are executable |
| Jobs | `0001_jobs.sql` contains `deferred`, separate `claim_count`/`failure_count`, PostgreSQL-clocked SQLx job operations, the worker-facing `JobService` facade, type-aware feed-rebuild/source-sync/article-backfill dispatch, lease-fenced credential-refresh dispatch when a transport is injected, and shutdown-aware heartbeat/outcome execution | Removal of compatibility `now` parameters remains future work |
| Configuration | Environment-only `AppConfig` with role, lease, cache, public RSS URL, admin, and optional-asset validation; unknown owned settings are rejected and legacy archive names fail with a migration hint; the supervisor consumes the parsed role and policy values | QR-attempt policy is currently bounded in the application manager; durable multi-replica attempt storage remains future work |
| Persistence | Job/source-scheduling/article/sync-run/feed-cache/feed-token tables, their PostgreSQL repositories, shared job/source/article/sync-run/feed-cache transaction boundary, account leases, feed-build leases, and encrypted WeRead account credential records with optimistic versions | QR attempts are intentionally process-local; durable encrypted attempt storage and remaining transaction-scoped views are future work |
| Acquisition/web/RSS | Public WeChat identity resolution, a validated public article URL, capability-typed browser sessions, concrete public Thirtyfour navigation/extraction with bounded pacing/scroll and expected-timezone validation, authenticated WeRead article-list transport through an admin-enrolled cookie and account lease, source-sync finalization through an injected acquisition port, feed rebuild orchestration plus its atomic worker handler, pure RSS renderer, public tokenized feed route, API liveness/readiness and browser-worker readiness routes, single-admin source/panel routes, and the WeRead QR login transport | Login/QR exchange is executable; durable multi-replica QR attempt storage remains future work |
| Archive | Conservative HTML allowlist sanitizer, deterministic content hashing, bounded anonymous image acquisition with article `Referer`/`Origin`/User-Agent, PostgreSQL binary persistence, add-time checksum/raw-byte deduplication, stable URL rewriting, asset serving, and age/size/orphan maintenance | Automatic missing-asset repair, refresh-lineage controls, and local-directory/S3 backends remain future work; the target follow-up contract is documented in [docs/ASSET_CACHING.md](docs/ASSET_CACHING.md) |

Environment variables in this document are parsed into `AppConfig`, and
`application::runtime::RuntimePlan` is the side-effect-free boundary that
derives role-specific component plans. It does not open connections, start
listeners, or spawn tasks; `RuntimeSupervisor` consumes it before constructing
those side effects. The loader has an explicit allowlist for application-owned names
and rejects unknown names within those prefixes so misspellings cannot be
silently ignored, while ordinary container variables such as `PATH` remain
permitted.

Application-owned prefixes are `APP_`, `HTTP_`, `WORKER_`, `JOB_`, `ACCOUNT_`,
`SOURCE_`, `RSS_`, `FEED_`, `PACING_`, `SCROLL_`, `ASSET_`, `ADMIN_`, `SESSION_`,
`CREDENTIAL_`, `QUIET_`, `WEBDRIVER_`, `BROWSER_`, `WEREAD_`, and
`DATABASE_POOL_`, plus
the exact `DATABASE_URL`.
The legacy `ARCHIVE_` names remain recognized only for a deprecation error or
explicit compatibility migration. Unknown variables under an owned prefix are
startup errors; unrelated environment names are ignored.

## Runtime shape

The first runtime is a modular monolith, but each process has an explicit role
set: `api`, `scheduler`, `worker`, or `all`. The supervisor accepts scheduler
and source-sync worker execution before a WeRead account is configured; each
job selects an encrypted cookie enrolled through the admin panel. Without a usable account, the job records a
warning and a scheduled failure, then the source is reconsidered on its next
due interval. Kubernetes may scale API
and worker processes independently
so RSS traffic does not implicitly increase upstream-fetch concurrency. The
current feed-rebuild worker is database-only; a browser sidecar is required
for worker processes because source synchronization is composed before an
account is enrolled and selects credentials when a job runs.
PostgreSQL is the coordination point, so multiple instances can run at the
same time without executing the same active job.

```text
RSS/admin clients -> API replicas -----------------+
                                                     |
Scheduler replicas -> due-source enqueue -----------v---+
                                                     PostgreSQL
Worker replicas <-> durable job/account leases -----^---+
       |
       +-- colocated private WebDriver sidecar per worker Pod
```

The browser endpoint is private to the application network. Local browser
capacity is bounded by each process, while authenticated WeRead operations are
serialized across all replicas by a PostgreSQL account lease. The account lease
is distinct from a source job lease because different source jobs may use the
same WeRead account. Version one may expose only one configured account, but it
still assigns that account a stable identifier and uses the distributed lease.
Application and browser sidecar timezone configuration must use the same IANA
timezone, such as `Asia/Shanghai`.

Public WeChat article pages use a separate ephemeral browser-session class.
Those sessions start with a clean profile, receive no WeRead credentials or
cookies, validate navigation and redirect hosts against the WeChat allowlist,
and are destroyed after use. Authenticated account sessions must never be
reused for public article extraction.

## Data flow

### Add a source

1. The API receives an article URL, a normalized `book_id`, or both.
2. If a book ID is supplied it is authoritative. Otherwise, if the URL already
   carries `__biz`, identity acquisition resolves it without network access;
   short URLs use a clean public browser session to capture the validated final
   URL and page source.
3. It extracts `biz` from the final URL or narrow page-source fallbacks,
   decodes the numeric `bid`, and derives `MP_WXS_<bid>`. A supplied display
   name overrides the resolved public-account name; book-only sources use the
   book ID as a stable fallback name.
4. The source repository stores the source and creates its initial schedule;
   an article URL is nullable when only a book ID was supplied.
5. A deduplicated `source_sync` job becomes eligible for execution.

### Edit or delete a source

1. The authenticated admin page loads one source by durable ID and submits a
   complete editable configuration to `PUT /api/admin/sources/{id}`. Omitted
   fields retain their current values; nullable article URL and account ID
   fields can be explicitly cleared.
2. The source transaction validates and trims the new identity, locks the
   current row, advances the feed revision only when feed-visible fields
   changed, and preserves scheduler timestamps and gates.
3. `DELETE /api/admin/sources/{id}` removes source-owned jobs and deletes the
   source; PostgreSQL cascades its articles, sync history, feed cache, and
   feed token. All three routes require the admin session, and mutations also
   require CSRF.

### Synchronize a source

1. A scheduler transaction locks eligible due sources with `FOR UPDATE SKIP
   LOCKED`, verifies that no active source-sync job exists, inserts a job, and
   records the scheduling reservation atomically.
2. A worker claims one queued job with a PostgreSQL row lock and lease.
3. The quiet-hours policy is checked before any upstream work begins.
4. The authenticated WeRead adapter loads the account context and article
   list, applying the pacing policy between upstream operations.
5. Each article URL is fetched separately from the public WeChat article page.
   This content fetch does not receive WeRead credentials and does not depend
   on the account login; it uses bounded waits and controlled scrolls for
   lazy-loaded content, then normalizes metadata, HTML, and asset references.
6. HTML is sanitized. In the default disabled mode, approved external asset
   URLs remain in the article. When the database asset-cache backend is
   enabled, the target asset path fetches each image with a separate anonymous
   HTTP request using the page's `Referer`, `Origin` when required, and
   User-Agent, deduplicates it while adding the asset, and rewrites successful
   records to stable local media URLs. It does not send WeRead cookies or
   acquire an account lease for public image hosts. Automatic repair of
   evicted bytes or expired signed URLs remains future work; the current asset
   route returns a bounded retryable miss without re-acquiring the article.
7. Browser and network acquisition finishes before the final database
   transaction begins. Lease heartbeats continue independently while upstream
   work is in progress.
8. Outside a transaction, the service merges current RSS input with normalized
   changes and renders a candidate for the expected next feed revision. A short
   persistence `UnitOfWork` then verifies the live job fencing token and expected
   base revision, upserts articles and archive records, advances the source feed
   revision, stores that candidate, updates the sync run and source schedule,
   and marks the job successful in one transaction. A concurrent revision
   change aborts this commit and causes a fresh candidate to be built. Asset
   metadata is included only when optional asset archiving is enabled.
9. If the final transaction fails, none of its writes or job completion become
   visible. The still-live or recoverable job can safely retry the idempotent
   workflow.

All synchronization work must be idempotent. A worker can crash after an
external asset write or at the final transaction boundary; retrying must not
create duplicate articles or content versions. If optional asset archiving is
enabled, asset writes must be deduplicated as well.

### Serve RSS

1. The feed route reads one cached XML document from `feed_cache`.
2. A fresh cache is returned immediately with its ETag.
3. An expired or missing cache invokes the database-only rebuild service. The
   rebuild lease and fenced publication ensure that concurrent requests do not
   overwrite one another, and the request reads the fresh published bytes back
   before returning them.
4. If another builder already owns the lease, the request polls for a bounded
   period. If rebuilding fails or that period expires, an expired cache remains
   available as a stale fallback; a true miss returns `503` with `Retry-After`
   and a deduplicated background rebuild is retained when possible.

RSS requests never start browser work or wait for a synchronization job.

## Transaction ownership

Cross-repository atomicity is provided by a persistence-level `UnitOfWork`, not
by a job-specific transaction. A unit of work owns one SQLx transaction and
exposes transaction-scoped source, article, sync-run, feed-cache, and job
repositories. Application code sees repository interfaces and never receives a
raw SQLx transaction.

Long browser operations, randomized waits, asset downloads, and XML generation
from a large input must not hold database row locks. A synchronization worker
first performs acquisition and normalization, then opens a short unit of work
for final persistence and fenced job completion. Job heartbeats use the pool on
a separate connection and stop the handler if ownership or fencing is lost.
Dropped or failed units of work roll back automatically; only an explicit
`commit` publishes all changes.

The target general job-queue port exposes enqueue, claim, heartbeat, and reads.
Worker outcomes (`succeeded`, `retry_wait`, `deferred`, cancellation, and
failure) are committed through the transaction-scoped unit-of-work outcome view
because they may also write a sync run, source gate/cooldown, revision, or cache.
Expired-lease recovery is a dedicated atomic persistence operation for the same
reason. `JobQueue`, `JobOutcomeTransaction`, and `ExpiredJobRecovery` now make
these boundaries executable for PostgreSQL and the in-memory test repository.
The all-in-one `JobRepository` and `JobRepositoryTransaction` remain temporary
compatibility interfaces; new application services must depend on the narrower
ports and must not receive an independently committing completion port.

## PostgreSQL job queue

The planned `jobs` table represents individual executions and contains:

```text
id, job_type, source_id, status, priority, run_after, claim_count,
failure_count, max_attempts, lease_owner, lease_token, lease_until, heartbeat_at, started_at,
finished_at, last_error, payload_json, dedupe_key, created_at, updated_at
```

Active-job deduplication uses a partial unique index over `dedupe_key` for
`queued`, `running`, `retry_wait`, and `deferred` jobs. Workers claim jobs with
`SELECT ... FOR UPDATE SKIP LOCKED`, set an instance-specific lease and a new
per-claim fencing token, and periodically extend it. Heartbeats and terminal
updates must compare both the owner and token so an old worker cannot mutate a
later claim by the same instance. Expired leases are returned to the queue.

PostgreSQL server time is authoritative for every distributed job decision.
Production claim, heartbeat, live-lease checks, retry/defer timestamps, and
expired-lease recovery derive one statement-local timestamp from
`clock_timestamp()` (or an equivalent database-owned clock) instead of trusting
a caller-provided wall clock. `run_after` eligibility is compared with that same
database time. Application-supplied clocks remain valid for pure domain tests
and the in-memory repository, but production SQL returns persisted database
timestamps and rehydrates domain values from them. Database time offset is
monitored operationally; replica clock skew cannot shorten another worker's
lease.

The current job-repository compatibility interface still accepts a caller `now`
so the existing in-memory tests and callers remain source-compatible. PostgreSQL
does not bind that value into lease-sensitive SQL: `claim_next`, `heartbeat`,
`succeed`, `defer`, `retry`, `fail`, `cancel`, and `recover_expired` use a
statement-local `clock_timestamp()`. The eventual queue-port/`UnitOfWork` split
will remove those compatibility parameters. Retry policy will then supply a
duration, which SQL adds to `db_now`; quiet-hours deferral will supply an
absolute instant calculated from an authoritative database-time sample and the
configured IANA timezone.

Expected state transitions:

```text
queued -> running -> succeeded
queued -> running -> retry_wait -> running
queued -> running -> deferred -> running
queued/running -> failed
```

`retry_wait` represents a retryable execution failure and consumes the bounded
failure budget. `deferred` represents a non-failure eligibility boundary such
as quiet hours and sets `run_after` to the next eligible instant without
consuming that budget. Durable `claim_count` is retained for observability,
while `failure_count` is compared with `max_attempts`; a claim and a failure are
not the same event. Expired-lease crash recovery increments `failure_count` so a
worker that repeatedly disappears cannot loop forever.

Job claiming accepts an allowed-job-type filter. During quiet hours, workers may
claim local jobs such as `feed_rebuild`, but they do not claim `source_sync`,
`article_backfill`, or any other job that can contact an upstream service.

Transient browser/network errors use bounded exponential retry. Authentication
expiry allows one refresh and one retry. Risk-control or verification states
stop the job and move the source to an operator-blocked scheduling state; they
do not trigger a retry loop. Quiet hours are a scheduling and politeness
boundary, not a way to evade verification or anti-automation controls.

### V1 queue transport decision

Version one keeps the custom PostgreSQL `jobs` table as the authoritative
application queue. It owns the durable job lifecycle, active-job
deduplication, retry and failure counters, quiet-hours deferral, leases,
fencing tokens, and the transaction-scoped completion contract. The
cache-first `FeedService` invokes the database-only rebuild capability for
missing or expired caches and enqueues `feed_rebuild` rows through this table
using the canonical `feed_rebuild:{source_id}` key only when request-time
rebuilding cannot complete.

PGMQ is a possible future transport optimization, not a version-one
replacement for the `jobs` table. If it is evaluated later, the safe hybrid
shape is:

```text
application jobs table = authoritative lifecycle, dedupe, retry, lease, fence
PGMQ message           = optional durable wakeup/transport for a job id
```

A future PGMQ adapter must preserve the following rules:

1. The `jobs` row remains the source of truth. A PGMQ visibility timeout must
   not be treated as the application's lease or fencing token.
2. A message should carry only a durable job identifier (and optionally a
   version), never credentials or a second copy of mutable job state. Workers
   must re-read and claim the `jobs` row before executing work.
3. Enqueueing a job and publishing its wakeup must be made transactionally
   consistent, or an outbox/reconciliation path must repair either side. A
   message that arrives twice is harmless because job claiming and active
   deduplication remain authoritative.
4. PGMQ extension or SQL-only installation, version compatibility, migration
   ownership, observability, and recovery behavior must be proven in a
   separate deployment experiment before adding it as a required dependency.

Until those conditions are satisfied, adding PGMQ would increase deployment
and migration surface without removing the correctness responsibilities
already handled by the custom table.

## Source scheduling and account leases

An enabled source also has an explicit scheduling gate:

```text
ready | authentication_required | risk_controlled
```

`enabled=false` is the operator-controlled subscription pause. Only
`enabled + ready + due` sources are eligible for automatic synchronization.
Successful completion advances `next_fetch_at` in the same unit of work as job
completion. Retryable failures remain represented by the active `retry_wait`
job. Exhausted ordinary failures advance `next_fetch_at` by a configured failure
cooldown; authentication and risk-control outcomes change the scheduling gate
and require an explicit successful login or operator action before automatic
work resumes. This prevents a terminal job from being recreated immediately
after its active deduplication key is released.

The scheduler repository owns a single atomic
`enqueue_due_sources(limit, reservation_for, quiet_hours)` operation. It samples
the PostgreSQL clock inside its transaction, evaluates the supplied quiet-hours
policy against that timestamp, and returns without source writes when the
window is active. Otherwise it locks due source rows with `SKIP LOCKED`,
excludes sources with an active source-sync job, inserts jobs with the canonical
`source_sync:{source_id}` deduplication key, and records the scheduling
reservation in the same transaction. Scheduler replicas never implement this
as a read-list followed by unrelated insert calls. The application owns the
policy configuration; the repository supplies only the authoritative timestamp
and atomic execution boundary.

Each configured WeRead account has a stable `account_id`. Authenticated list,
URL-recovery, login, and credential-refresh operations acquire a durable
`account_leases` row containing owner, fencing token, lease expiry, and
heartbeat time. Public article-page sessions do not acquire that account lease
and never receive account secrets. Account lease loss cancels authenticated
browser work before another upstream request is made. Account lease acquisition,
heartbeat, expiry, and takeover also use PostgreSQL server time; an application
replica does not decide from its local clock that another account lease expired.

## Local PostgreSQL development

PostgreSQL can be run locally as a disposable or persistent Docker container.
This is intended for repository integration tests and manual development; it
is not a production database deployment. The application and future migration
tests should connect through the same `DATABASE_URL` path used in Kubernetes.

Create a named volume and start a development-only PostgreSQL instance:

```sh
docker volume create werrss-postgres-dev-data
docker run --detach \
  --name werrss-postgres-dev \
  --env POSTGRES_USER=werrss \
  --env POSTGRES_PASSWORD=werrss-dev-only \
  --env POSTGRES_DB=werrss \
  --publish 5432:5432 \
  --volume werrss-postgres-dev-data:/var/lib/postgresql/data \
  --health-cmd='pg_isready -U werrss -d werrss' \
  --health-interval=2s \
  --health-timeout=5s \
  --health-retries=15 \
  postgres:16-alpine
```

Wait until the container reports healthy before starting the application:

```sh
docker inspect --format '{{.State.Health.Status}}' werrss-postgres-dev
```

For local development, configure the process with environment variables. The
password below is intentionally a development-only example:

```sh
export DATABASE_URL='postgresql://werrss:werrss-dev-only@127.0.0.1:5432/werrss'
export DATABASE_POOL_MIN_CONNECTIONS=1
export DATABASE_POOL_MAX_CONNECTIONS=5
```

All PostgreSQL SSL modes, CA certificates, client certificates, private keys,
passwords, and related connection options must remain in `DATABASE_URL` and
its query parameters. The Rust process passes this URL to SQLx unchanged; it
does not add separate PostgreSQL SSL environment variables. A local container
normally runs without TLS, so production credentials and certificate paths
must not be copied from this example.

The container can be stopped and restarted without losing data because the
named volume is separate from the container:

```sh
docker stop werrss-postgres-dev
docker start werrss-postgres-dev
```

When the local database is no longer needed, remove the container and, only if
the development data is disposable, remove its volume too:

```sh
docker rm --force werrss-postgres-dev
docker volume rm werrss-postgres-dev-data
```

The current implementation includes the source scheduling/revision fence,
`feed_cache`, `account_leases`, and `feed_build_leases` tables and their
PostgreSQL repositories. The scheduler repository atomically reserves due
sources and inserts canonical source-sync jobs across replicas. The application
uses SQLx's embedded `Migrator` to discover files
under `migrations/`, record applied versions and checksums in
`_sqlx_migrations`, and apply only pending migrations. The PostgreSQL
integration test uses `#[sqlx::test]`, which creates an isolated temporary
database and applies the application's embedded migrator automatically. Set
`DATABASE_URL` to a PostgreSQL administrative connection and run it with:

```sh
export DATABASE_URL='postgresql://werrss:werrss-dev-only@127.0.0.1:5432/werrss'
cargo test --locked --test postgres_job_repository -- --nocapture
```

For faster local execution, install `cargo-nextest` and run the integration
test targets with bounded parallelism:

```sh
cargo install cargo-nextest --locked
cargo nextest run --locked --tests -j 16
```

SQLx integration tests use `#[sqlx::test]`, so each test receives an isolated
temporary database and can run concurrently without sharing application data.
The repository's `.config/nextest.toml` keeps the global `-j 16` setting for
fast unit-test execution while placing the API and PostgreSQL-backed test
binaries in a `postgres` test group capped at sixteen concurrent isolated
database setups. This keeps database concurrency bounded independently if the
global setting is raised later. Lower that group limit for a smaller or shared
PostgreSQL service. The ignored real-browser test remains a separately
controlled WebDriver test and is not part of the normal nextest run.

Mutation testing is also part of the validation workflow for behavior-heavy
application code. Install `cargo-mutants` once, then use nextest as its test
runner. The focused command below checks the browser-backed source-sync bridge
without requiring an external database:

```sh
cargo install cargo-mutants --locked
cargo mutants --test-tool nextest --jobs 4 --timeout 180 \
  --file src/application/source_sync_acquirer.rs -- --lib
```

`cargo-mutants` runs a baseline before applying mutations and reports caught,
missed, timed-out, and unviable mutations. A mutation-testing check is
successful only when there are no missed mutations; unviable mutations are
compiler-invalid variants and should be reviewed if their count changes.
Keep mutation-process concurrency lower than the `cargo nextest -j 16`
database-test setting because each mutation is a separate build/test process.
For repository or other database-backed mutations, provide the same external
administrative `DATABASE_URL` used by the SQLx tests and omit `-- --lib` so
the relevant integration tests are included. Do not put private connection
details in tracked files.

`0001_jobs.sql` is part of the embedded migration history and remains immutable
after publication. It contains the corrected `deferred`, `claim_count`, and
`failure_count` model, with active indexes that include `deferred`. No release
has been published yet, so there is no legacy `attempts` column or compatibility
trigger to retain. After publication, all schema changes must use a new forward
migration; the policy is recorded in `migrations/README.md`.

Each successful test run cleans up its temporary database; a failed run may
leave it in place for diagnosis. The configured PostgreSQL role must be allowed
to create databases. Future integration tests should use the same harness and
verify feed-cache updates alongside job claiming, lease fencing, recovery, and
active-job deduplication. CI should start an equivalent ephemeral service
rather than depend on a developer's named volume.

## Pacing and quiet hours

The pacing policy is explicit and shared by the WeRead and article-page
acquisition adapters. It supports separate delays for:

- before an upstream request;
- before a new article-page navigation;
- between page actions such as extraction and scrolling;
- after a page has been scrolled, to allow lazy content to settle.

The default distribution is a bounded, truncated normal distribution with a
configured mean, standard deviation, minimum, and maximum. Values are clamped
to the configured bounds, and tests use a seeded random source. This is for
request pacing and load reduction; it must not be described or tuned as an
anti-detection mechanism.

Scrolling is a small configurable sequence of bounded viewport increments with
settling waits. Its purpose is to trigger legitimate lazy-loaded article
content, not to simulate arbitrary human input. The policy must cap total
scroll distance, action count, and page duration. Version one caps
`SCROLL_MAX_STEPS` at 64 actions and `SCROLL_MAX_PIXELS` at 1,000,000 CSS
pixels before the policy reaches the browser adapter, preventing malformed
environment values from causing an unbounded allocation or page operation.

Quiet hours use an IANA timezone and a local start/end time, for example:

```text
timezone: Asia/Shanghai
quiet window: 23:00-07:00
```

The scheduler must not enqueue new upstream fetch jobs during the window, and a
worker claim filters out queued upstream job types. A worker checks again
immediately before each request and page navigation, so a quiet-window
transition is respected even for a long-running job. The current bounded
operation may finish; the worker then transitions the job to `deferred` with
`run_after` set to the end of the quiet window. This is not an error and does not
consume the failure retry budget. Feed reads, local feed rebuilds, and cache
serving continue normally.

All application replicas use the same configured timezone and clock policy.
The browser sidecar must contain timezone data and set `TZ` to the same IANA
value. A browser smoke test should verify the browser-visible local timezone;
container timezone alone is not considered verified until that test passes.

## Feed cache

The `feed_cache` table has one row per source:

```text
source_id, xml_bytes, etag, generated_at, expires_at,
feed_revision, content_hash, updated_at
```

The default freshness period is 30 minutes. Successful article fetches
proactively rebuild the cache. Source edits, article updates, URL backfills,
content changes, and optional asset rewrites invalidate or rebuild the related
row by incrementing `sources.feed_revision` in the mutation transaction. A row
is fresh only when both `expires_at > now` and its revision equals the source
revision.

The current persistence slice implements source configuration, normalized
article persistence, the cache read, and the final revision/fence
compare-and-swap publication. Callers must still provide the source revision
and normalized rendered candidate through application ports. The pure renderer
is implemented in `src/rss/renderer.rs`; the cache-first feed service is
implemented in `src/application/feed_service.rs`; database-only feed rebuild is
implemented in `src/application/feed_rebuild_service.rs`. Public feed-token
lifecycle is implemented in `src/domain/feed_token.rs`,
`src/persistence/repositories/feed_token_repository.rs`, and
`src/application/feed_token_service.rs`; the public route composition is
implemented in `src/web/api.rs`; the authenticated administrative API and panel
are composed in `src/web/admin.rs` and `src/web/ui.rs`.

Feed tokens are 32 random bytes encoded as unpadded base64url. PostgreSQL
stores only the SHA-256 digest in `feed_tokens`, with one current row per
source. Issuing a token returns the raw value only to the administrative
caller; rotation replaces the digest and immediately invalidates the old
value, while revocation is idempotent. Invalid, unknown, and revoked values
must map to the same public HTTP response so token existence is not disclosed.
Token lifecycle does not change `sources.feed_revision` or `feed_cache`; after
resolution, the feed route passes the source id to `FeedService` and serves
the existing cached XML.

Cache replacement uses compare-and-swap semantics. A renderer records the
source revision of its database snapshot, and the repository stores the result
only if the source still has that revision and the existing cache is not newer.
If either check fails, the rendered bytes are discarded and a deduplicated
rebuild for the current revision remains eligible. This prevents a slow replica
from overwriting newer XML. For a synchronization mutation built from base
revision `R`, the final unit of work verifies `R`, atomically advances the source
to `R+1`, and stores only the candidate labeled `R+1`.

Single-flight cache generation uses a durable `feed_build_leases` row keyed by
`source_id`; it does not use a session-level advisory lock or hold a database
connection while XML is rendered. Acquisition is a short transaction that
inserts or takes over an expired row and returns `owner`, a fresh fencing token,
and `lease_until`, all based on PostgreSQL server time. The owner commits that
transaction, reads a revisioned RSS snapshot, samples PostgreSQL time for the
candidate timestamps, renders outside any transaction, and heartbeats the build
lease if necessary. A short final `UnitOfWork` verifies the build token and
source revision, replaces the cache, and releases the build lease atomically.
Using database time for `generated_at` keeps same-revision cache ordering and
freshness decisions independent of replica clock skew. Lease loss or revision
conflict discards the candidate.

```text
feed_build_leases:
source_id, lease_owner, lease_token, lease_until, heartbeat_at, created_at,
updated_at
```

When an expired cache exists, non-owners wait for the lease owner's fresh
result for the configured bound and then return the expired bytes only if the
fresh result is unavailable. On a true cache miss, a non-owner does not render
concurrently: it performs the same bounded poll and otherwise returns a typed
temporary-unavailable response with `Retry-After`. Expired build leases are
safely takeable by another replica.

The feed endpoint returns `ETag`, `Last-Modified`, and `Cache-Control`. The
initial route uses `max-age=0, must-revalidate` for fresh responses because the
cache-read port currently exposes freshness as a boolean rather than the exact
database-clocked remaining lifetime. Stale responses use `max-age=0` and an
explicit bounded `stale-while-revalidate` directive, while still honoring
`If-None-Match`. The cache-read contract should expose the remaining lifetime
before a later version advertises a positive `max-age`; the database cache is
an application cache, not a replacement for HTTP conditional requests.

## Module boundaries

### Domain

Pure business concepts and invariants. Domain modules do not depend on Axum,
Thirtyfour, SQLx, or concrete storage. Article storage identity is the pair
`(source_id, review_id)`; `review_id` is the stable upstream identity within a
source. URLs and content may be absent in a partial list observation and are
merged with previously known detail data rather than treated as deletions.
Article observations carry a monotonic version allocated before upstream work
starts; persistence ignores an older version so out-of-order workers cannot
regress newer RSS content. `fetched_at` records completion time and is never
used as the ordering fence.
Jobs and sync statuses are explicit types so retry and risk-control behavior
cannot be represented as arbitrary strings.

### Application

Use-case orchestration. Application services call repository and acquisition
interfaces but do not contain SQL or browser selectors. `Scheduler` only
enqueues work and applies quiet-hours eligibility. `SyncService` executes
claimed work and re-checks quiet hours between upstream operations.
`ArchiveService` owns the content pipeline. `JobService` owns leases and
transitions. `FeedService` owns conditional reads, fresh/stale/missing
decisions, on-demand database-only rebuilding, and deduplicated retry
enqueueing over the persisted cache.
Single-flight cache population is coordinated by the durable feed-build lease,
and database-only rebuild orchestration is implemented by
`FeedRebuildService`. Application services receive a `UnitOfWorkFactory` for
atomic final writes; they do not compose independent repository transactions.
`FeedTokenService` now owns opaque token issue/rotate, strict request parsing,
hash-only repository access, and idempotent revocation. `SourceService` now
uses a narrow transaction-scoped enqueue view to create an
eligible source and its initial `source_sync` job atomically.

### Acquisition

Browser and WeRead protocol adapters. Thirtyfour/WebDriver details are confined
here. The existing Python fetching path does not include article-content
fetching, so Rust keeps the two upstream paths explicit: WeRead credentials are
used only for account/session and article-list operations, while the rendered
article-page adapter fetches public article content without credentials. Neither
adapter exposes raw protocol details to the application layer. The pacing module
is the only owner of randomized wait generation and scroll policy; individual
adapters must not invent their own delays.

The browser abstraction exposes different capabilities for authenticated and
public sessions. The public article adapter accepts only a validated WeChat URL,
uses a clean ephemeral profile, and validates the final URL after redirects.
The type system must make it impossible to pass credentials or an authenticated
session to that adapter accidentally.

The concrete capability contract is:

- `VerifiedWechatArticleUrl` is constructed only from `https` URLs whose
  normalized host is exactly `mp.weixin.qq.com`; user information, fragments,
  non-default ports, and ambiguous encoded hosts are rejected. The final URL is
  revalidated after every navigation or redirect before extraction. The
  domain-side value object is implemented in `src/domain/source.rs`; the
  acquisition ports accept it directly, and the public WebDriver adapter
  revalidates the browser-observed URL before extraction.
- `WebDriverIdentityResolver` consumes only a `PublicBrowserSession`. It
  resolves long URLs locally, then uses the validated final URL and rendered
  source fallbacks (`biz`, `msg_link`, canonical metadata, and embedded public
  links) for short URLs. It emits `MP_WXS_<bid>` plus optional title/account
  metadata; malformed IDs and structural verification pages are typed errors.
- `PublicBrowserSession` is a non-cloneable fresh WebDriver session with no
  imported profile, cookies, local storage, credential handle, or account-lease
  guard. It is destroyed after the public operation.
- `AuthenticatedBrowserSession` is a distinct non-cloneable capability created
  only with a live `AccountLeaseGuard` for one stable account ID. The guard owns
  cancellation state; losing its fence prevents another authenticated request.
- `AuthenticatedBrowserSession::prepare_request` performs a PostgreSQL-clocked
  heartbeat and returns a one-request authenticated capability. An expired
  lease cannot be handed to the protocol adapter; the application must repeat
  preparation for each request or bounded protocol operation.
- `ArticlePageFetcher` accepts `VerifiedWechatArticleUrl` and consumes a
  `PublicBrowserSession`. Consuming the capability makes the one-operation
  lifetime explicit and releases local browser capacity when the fetch future
  completes or is cancelled. `WeReadAdapter` accepts only the authenticated
  request capability returned by `prepare_request`; neither API accepts a
  generic session. The pure `parse_article_list_payload` parser accepts current
  and legacy response envelopes, classifies authentication/risk-control
  business errors, and emits only verified public article URLs. Transport, QR
  exchange, refresh, and request pacing remain outside this parser.

The executable acquisition boundary currently includes `BrowserPool`, a
Tokio-semaphore process-local capacity limit, the storage-neutral
`AccountLeaseStore` port, `AccountLeaseGuard`, the two non-cloneable session
capabilities, `WebDriverFactory`, the public article fetcher, its
`PacingController`, and expected browser-timezone validation. A public session
has no account or credential state; an
authenticated request capability is created only after a durable account
heartbeat. A failed heartbeat permanently cancels the capability. The public
adapter creates a Thirtyfour session, applies the configured browser profile,
navigates to the verified URL, rejects an unsafe final URL, resolves short
links through narrow page-source fallbacks, executes bounded
navigation/action/settling waits and downward scrolls, and parses common
rendered WeChat metadata/body selectors. The health monitor probes the sidecar
status endpoint before opening a short-lived public session, publishes
WebDriver and timezone component status, and gates browser-backed worker jobs;
when `expected_timezone` is configured, session creation validates the
browser-visible timezone before returning the public capability. IANA links are
canonicalized by the browser's own `Intl` implementation so a valid alias is
not rejected; the real-browser diagnostic compares the browser-reported value
with that canonical result. The adapter also exposes an environment diagnostic,
and the optional sidecar test verifies the configured timezone and other
measurable profile values.
Environment/CAPTCHA verification pages are returned as a distinct terminal
acquisition result; the service must not attempt to bypass them. The complete
browser-session environment is part of the upstream risk-control input: the
engine, operating system, WebDriver mode, fresh profile, headers, viewport,
and network identity can all differ. An otherwise valid public page may be
returned as an environment-verification page by a server-side automated
Chromium session while it renders normally in a local interactive Chromium
session or a server-side Firefox session. This is not evidence that the URL is
invalid, and it must not be hidden by treating the verification document as an
article. Operators may select the sidecar engine explicitly with
`BROWSER_ENGINE`; automatic cross-engine retries and automation-evasion
changes are out of scope.

### Persistence

PostgreSQL connection management, migrations, transactions, and repositories.
Repositories own SQL and map rows to domain values. Job claiming, leases,
deduplication, distributed account leases, feed-cache reads/writes, and the
optional database asset cache are persistence responsibilities. `UnitOfWork`
is the only cross-repository transaction owner.

### Archive

Sanitization is required and is implemented as a pure allowlist boundary in
`src/archive/sanitizer.rs`. `ArchiveService` in
`src/application/archive_service.rs` applies that policy, hashes the normalized
HTML with lowercase SHA-256, and returns the same deduplicated external image
URLs in first-seen order. Empty sanitized output has no content hash, allowing
partial list observations to remain distinct from archived article content.
The optional database asset backend now implements checksum-based
deduplication, bounded anonymous HTTP acquisition, stable URL rewriting, and
binary-byte eviction. Its target and follow-up behavior is specified in
[`docs/ASSET_CACHING.md`](docs/ASSET_CACHING.md): `database` is the canonical
PostgreSQL-backed backend, `ASSET_MAX_SIZE_MB` defaults to 10, and aggregate
size accounting includes only present raw binary bytes. The pure
`ArchiveService` still never performs network I/O; the separate asset fetcher
applies the bounded SSRF-safe URL policy and idempotent writes.

### RSS

Pure rendering from normalized records. It does not fetch upstream content,
open browsers, or decide when synchronization occurs. It emits stable GUIDs,
escaped XML, archived HTML, approved external asset URLs by default, and an
ETag/content hash. If optional asset archiving is enabled, it may instead emit
rewritten local asset URLs.

### Web

REST and UI boundary. Administrative endpoints are protected; RSS feed URLs
use opaque feed tokens and can be consumed without admin credentials. Tokens
and refresh credentials are never serialized into API responses or logs.
Missing admin authentication configuration never means anonymous administrator
access: administration is either explicitly enabled with complete credentials
or its routes are not registered.

## Planned dependencies

The manifest declares the dependencies used by the implemented foundation and
reserved for the remaining modules.

- Tokio for asynchronous runtime and task coordination.
- Axum and Tower HTTP middleware for the API boundary.
- SQLx with PostgreSQL for the pool, transactions, repositories, and embedded
  migrations (`postgres`, `runtime-tokio-rustls`, and `migrate` features).
- Thirtyfour for WebDriver browser sessions, including typed capabilities,
  explicit waits, and request/page-load timeout configuration.
- Serde, URL, Base64, `scraper`, and `ego-tree` for normalized data and safe
  traversal of parsed article fragments.
- The `rss` crate for RSS 2.0 serialization, stable GUIDs, namespaces, and
  `content:encoded` output; the renderer hashes the resulting bytes for ETags.
- Tracing for structured diagnostics.
- Thiserror/Anyhow for typed boundary errors and application context.
- Secrecy for in-memory secret handling.

## Target version-one configuration

Configuration is loaded only from environment variables in the first version.
There is no application configuration file and no command-line override layer.
Kubernetes ConfigMaps and Secrets may inject environment variables, but the Rust
process consumes them through its environment. The implementation-status table
above distinguishes settings that are parsed from settings whose runtime
components are not yet composed.

The typed configuration loader groups and validates variables in these
categories:

```text
DATABASE_URL
DATABASE_POOL_MIN_CONNECTIONS / DATABASE_POOL_MAX_CONNECTIONS
WEBDRIVER_URL / BROWSER_ENGINE / WORKER_CONCURRENCY
BROWSER_USER_AGENT / BROWSER_LOCALE / BROWSER_VIEWPORT_WIDTH /
BROWSER_VIEWPORT_HEIGHT / BROWSER_EXTRA_ARGS /
WEREAD_ACCOUNT_ID / WEREAD_ARTICLE_LIST_URL
APP_INSTANCE_ID / HTTP_BIND / HTTP_PORT / LOG_LEVEL
APP_ROLES
APP_TIMEZONE / QUIET_HOURS_START / QUIET_HOURS_END
JOB_POLL_SECONDS / JOB_LEASE_SECONDS / JOB_HEARTBEAT_SECONDS /
JOB_MAX_ATTEMPTS
ACCOUNT_LEASE_SECONDS / ACCOUNT_HEARTBEAT_SECONDS
SOURCE_FAILURE_COOLDOWN_SECONDS
RSS_CACHE_TTL_SECONDS / RSS_STALE_WHILE_REVALIDATE_SECONDS /
RSS_CACHE_MISS_WAIT_MS / SERVER_ROOT_URL
FEED_BUILD_LEASE_SECONDS / FEED_BUILD_HEARTBEAT_SECONDS
PACING_* / SCROLL_*
ASSET_ARCHIVE_BACKEND / ASSET_CACHE_MAX_SIZE_MB /
ASSET_CACHE_MAX_AGE_DAYS / ASSET_MAX_SIZE_MB /
ASSET_USE_ABSOLUTE_URLS /
ASSET_MAX_COUNT_PER_ARTICLE / ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB /
ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS / ASSET_FETCH_TIMEOUT_SECONDS /
ASSET_MAX_REDIRECTS
ADMIN_ENABLED / ADMIN_USERNAME / ADMIN_PASSWORD / SESSION_SIGNING_KEY /
CREDENTIAL_ENCRYPTION_KEY
```

`ASSET_ARCHIVE_BACKEND` accepts `disabled`, `database`, and the compatible
`postgres` alias. Local-directory and S3 storage, automatic asset repair, and
asset refresh settings are roadmap items and are not accepted environment
variables in the current runtime.

`APP_TIMEZONE` is an IANA timezone name and defaults to `UTC`. `APP_ROLES`
defaults to `api`; API and worker startup additionally require
`SERVER_ROOT_URL` because either role may build a missing or expired feed
cache on demand.
`APP_INSTANCE_ID` may be omitted for local use; the loader then generates a
random per-process UUID so application replicas do not share job-lease
ownership. `WORKER_CONCURRENCY` defaults to `1`, and account/feed-build
leases default to 600 seconds with 60-second heartbeats. The source failure
cooldown defaults to 300 seconds, RSS stale-while-revalidate defaults to 60
seconds, and a cache miss wait defaults to 5 seconds. Required secrets and
connection strings fail startup when absent or invalid. `JOB_LEASE_SECONDS`
must exceed the heartbeat interval plus the maximum page-operation duration.
Pacing, worker concurrency, cooldown, stale-cache, and cache-miss values have
practical upper bounds before conversion to runtime durations. Diagnostics
expose names and validation failures, never secret values. Environment parsing
uses typed deserialization followed by domain validation.

Browser profile diagnostics are configured through `BROWSER_USER_AGENT` (an
optional fixed page User-Agent), `BROWSER_LOCALE` (default `zh-CN`),
`BROWSER_VIEWPORT_WIDTH` and `BROWSER_VIEWPORT_HEIGHT` (defaults `1280` and
`2000`), and `BROWSER_EXTRA_ARGS` (up to 32 whitespace-separated arguments).
The User-Agent must match the actual browser engine and installed version; it
must not be randomly changed between requests or made inconsistent with the
browser's other observable values. `APP_TIMEZONE` remains the expected
browser-visible timezone. The sidecar must set the same timezone, while the
real-browser diagnostic verifies the value rather than trying to change the
sidecar's operating-system timezone.
Extra arguments cannot override controlled locale, User-Agent, viewport,
headless, or profile settings, including Chromium `--user-data-dir` and
Firefox `-profile`; browser sessions therefore cannot opt into a persistent
browser profile through this setting.

`WEREAD_ACCOUNT_ID` is optional. When supplied, it is the default
panel-enrolled account. When omitted, source-sync selects an enabled,
unexpired account enrolled through the admin panel from PostgreSQL for each
unbound job, choosing randomly among usable accounts. A source-specific
account ID takes precedence. The adapter injects the encrypted cookie header enrolled through the
admin panel into a fresh authenticated browser session, opens
`https://weread.qq.com/web/shelf` in that same session, and stops with an
authentication-expired result if WeRead redirects away from the shelf. `WEREAD_ARTICLE_LIST_URL`
defaults to `https://weread.qq.com/web/mp/articles` and is accepted only as that exact HTTPS
endpoint without credentials, fragments, or a non-default port. Runtime source-sync listing holds
the account lease through its authenticated request,
applies the shared request pacing policy, then releases it before public article
fetching. The admin router also exposes a bounded WeRead QR login manager and
HTTP transport. A successful, identity-checked exchange provisions or replaces
the encrypted account record; provisioned credentials can also be refreshed by
the authentication application service. QR attempts remain process-local until
durable encrypted attempt storage is introduced.

### Reference: `we-mp-rss` browser anti-detection approach

The upstream `we-mp-rss` implementation is recorded here as a research
reference, not as a promise that its technique defeats WeChat risk controls.
Its Playwright controller combines a generated desktop/mobile User-Agent,
viewport, Chinese locale, explicit timezone, Chromium launch arguments such as
`--disable-blink-features=AutomationControlled`, and an initialization script
that changes several WebDriver-visible properties. Its article path waits for
DOM content, waits briefly for page stabilization, inspects the verification
text, extracts `#js_content`, and scrolls in bounded increments to trigger lazy
images. See its [browser controller](https://raw.githubusercontent.com/rachelos/we-mp-rss/main/driver/playwright_driver.py),
[article fetcher](https://raw.githubusercontent.com/rachelos/we-mp-rss/main/driver/wxarticle.py),
and [anti-crawler script](https://github.com/rachelos/we-mp-rss/blob/main/driver/anti_crawler_advanced.js).

Our first diagnostic slice adopts only the measurable profile inputs: fixed
User-Agent, viewport, locale, timezone verification, and explicit browser
arguments. It intentionally does not inject that JavaScript or claim to hide
`navigator.webdriver`; the diagnostic must remain attributable and must
continue to classify verification pages as a terminal upstream result.

`APP_ROLES` is a validated set containing `api`, `scheduler`, and/or `worker`;
`all` expands to all three. Scheduler and source-sync worker startup do not
require an enrolled account. Source-sync selects an active
enrolled account when each job runs; without one, the job records a warning and
waits for the source's next due interval. Worker concurrency is
configured independently from API replica count. `SERVER_ROOT_URL` is an optional
validated public HTTP(S) URL for generated RSS channel links and is required
when the worker role is enabled. `ASSET_USE_ABSOLUTE_URLS` defaults to `true`;
when database asset caching is enabled it also requires `SERVER_ROOT_URL` and
makes feed image links full URLs rooted there. Set it to `false` to retain
root-relative `/assets/{id}` links. Feed delivery rebuilds persisted caches
whose asset URL form does not match the configured mode. The current asset-cache
implementation accepts only `disabled | database` and defaults to `disabled`;
`database` is the
canonical PostgreSQL-backed value. `local` and `s3` are reserved for future
implementations. Local paths or object-store credentials are not part of the
first implementation; the target cache limits and fetch settings are defined in
[`docs/ASSET_CACHING.md`](docs/ASSET_CACHING.md).

The target asset-recovery admission limit is cluster-wide: all replicas must
use one PostgreSQL transaction-admission lock, check active-job deduplication
before capacity, and reserve public-repair capacity for internal refresh work.
Capacity waits use the existing non-failure deferred state, while refresh
eligibility is provider-confirmed and its durable attempt budget is carried
across replacement signed-URL versions. Permanent or exhausted repair lineages
are circuit-broken until a new asset observation is published.

The current loader requires a feed-build lease to exceed its heartbeat
interval. Once RSS rendering exposes a configurable maximum render duration,
the same validation must include that duration. `RSS_CACHE_MISS_WAIT_MS` is a
short bounded wait and never approaches browser or source-sync timeouts.

`ADMIN_ENABLED` defaults to false. When true, `ADMIN_USERNAME`,
`ADMIN_PASSWORD`, and an independent `SESSION_SIGNING_KEY` are required and
startup fails if any is missing. The first version has one environment-configured
administrator and does not provide user management. When false, management,
QR-login, and credential mutation routes are
not registered. Feed and health routes remain available. Session cookies are
`HttpOnly`, `SameSite=Lax` or stricter, and `Secure` when served over HTTPS;
state-changing UI requests require CSRF validation. Deployments terminate TLS
at the application or a trusted ingress and apply login rate limiting.

PostgreSQL pool sizing is configured with
`DATABASE_POOL_MIN_CONNECTIONS` and `DATABASE_POOL_MAX_CONNECTIONS`, then
applied to SQLx `PoolOptions`. All PostgreSQL SSL, certificate, private-key,
password, and related connection settings are carried through `DATABASE_URL`
and its query parameters. The application does not define separate PostgreSQL
SSL/certificate environment variables and must pass the URL through to SQLx
without exposing it in logs.

## Security and operations

- WebDriver is reachable only on the internal application network.
- Credential values are encrypted before PostgreSQL persistence.
- Encryption keys and administrative secrets come from deployment secrets.
- Browser sessions are closed on normal completion and failure.
- Public article sessions use fresh profiles and cannot access authenticated
  account sessions.
- The application and browser sidecar use the same explicit IANA timezone;
  sidecar images include `tzdata` and set `TZ`.
- Logs contain identifiers and error classifications, never access or refresh
  tokens.
- `/api/health` is process liveness and does not contact dependencies.
- `/api/ready` checks PostgreSQL with a lightweight query and returns `503` when
  the dependency is unavailable. Its JSON diagnostic reports only stable
  component status and the configured timezone.
- API readiness requires PostgreSQL but does not fail solely because the browser
  sidecar is unavailable; cached RSS remains serviceable. Browser and timezone
  health are reported as degraded components and prevent browser jobs from
  being claimed. A worker-only process may include browser availability in its
  own readiness condition.

## Testing direction

The implementation phase should add domain tests, fixture tests for current and
legacy upstream responses, fake-browser tests, real WebDriver container tests,
PostgreSQL repository tests, cache/ETag tests, lease-recovery tests, and a
Docker Compose end-to-end test. The optional ignored test in
`tests/real_browser.rs` exercises one real public WeChat page through a
Chromium WebDriver sidecar; it uses no credentials and is never required in
CI. It succeeds when content is extracted and also accepts the typed
`VerificationRequired` result when the upstream blocks the test environment;
it must never treat a verification page as article content. Run it only after
the sidecar is reachable through a local port forward:

```sh
WEBDRIVER_URL=http://127.0.0.1:4444 \
  cargo test --locked --test real_browser -- --ignored --nocapture
```

Set `BROWSER_ENGINE=firefox` when the sidecar uses Firefox. The same test
asserts the known article title after successful extraction, so a working
Firefox deployment exercises the Rust navigation and parser rather than only
checking sidecar availability.

The real-browser test also applies the optional browser profile variables and
prints the effective browser values. For example, an operator may run a
controlled comparison with values matching the installed sidecar browser:

```sh
APP_TIMEZONE=Asia/Shanghai \
BROWSER_LOCALE=zh-CN \
BROWSER_VIEWPORT_WIDTH=1280 \
BROWSER_VIEWPORT_HEIGHT=2000 \
BROWSER_USER_AGENT='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<installed-version> Safari/537.36' \
BROWSER_EXTRA_ARGS='--disable-blink-features=AutomationControlled' \
BROWSER_ENGINE=chromium \
WEBDRIVER_URL=http://127.0.0.1:4444 \
  cargo test --locked --test real_browser -- --ignored --nocapture
```

Do not interpret one successful or failed profile as proof of a universal
workaround. Compare one changed variable at a time and retain the printed
User-Agent, language, viewport, timezone, final URL, and verification result.

The requested browser dimensions are outer window dimensions. The effective
CSS inner viewport can be smaller because the driver or sidecar constrains the
window; the diagnostic therefore requires positive dimensions no larger than
the request rather than assuming exact equality. This keeps the test portable
across Chromium and Firefox sidecars while still detecting an ignored or
invalid profile.

Add pacing tests for bounds, seeded normal sampling, quiet-window boundaries,
timezone/DST behavior, and interruption between page operations. The optional
browser-sidecar test checks the browser's reported timezone, and the factory
rejects a configured timezone mismatch before returning a public session. Add
unit-of-work rollback/fencing tests, scheduler tests that prove terminal and
operator-blocked sources are not immediately recreated, non-failure deferral
tests, distributed account-lease contention tests, public session isolation/
redirect tests, and cache revision/CAS stampede tests. Add a PostgreSQL test
whose application replicas deliberately supply skewed local clocks and prove
that claim, heartbeat, and recovery still follow database time.

## Contract freeze and implementation order

The next implementation changes must make existing contracts executable rather
than add more empty module shells. Work proceeds in this order:

1. Complete the job queue slice around the now-executable queue/outcome/recovery
   ports. The repository supports allowed-kind claiming, while the initial
   `0001_jobs.sql` migration already adds `deferred`, separate claim/failure
   counters, and PostgreSQL-authoritative lease time. Replace the compatibility
   all-in-one interfaces only after the first worker uses the narrower ports.
   Use an expand/contract rollout for later schema changes so mixed replica
   versions remain safe; upgrade and concurrency tests are a gate.
2. Extend the shared `UnitOfWork` with transaction-scoped repository ports and
   complete the remaining source service and credential repositories plus their
   transaction-scoped views. Source identity/create/read and atomic initial
   source-sync enqueue and the injected-port source-sync finalization handler
   are executable;
   normalized article persistence,
   synchronization-run persistence,
   scheduling state, and feed-revision mutations are already executable. The
   revision-aware feed-cache publication view, account lease, and feed-build
   lease repositories are also executable. No source or feed application
   service may bypass these boundaries with convenience transactions.
3. Make `FeedService` executable over the persisted cache (cache-first
   delivery, on-demand database-only rebuilding, and deduplicated retry
   enqueueing are implemented). The worker supports committing its fenced
   outcome through `UnitOfWork`, and
   `FeedRebuildService::rebuild_for_job` now couples successful feed-cache
   finalization to that outcome. Add the remaining source/archive queries; the
   `FeedRebuildJobHandler` already maps pre-publication rebuild failures to
   explicit worker outcomes.
4. Build role-aware runtime composition and integrate the worker loop and
   heartbeat cancellation. The supervisor now covers the API, scheduler,
   database-only feed-rebuild, and account-selection-at-job-time authenticated
   source-sync worker paths. The shared browser-health monitor and worker claim
   gate are executable.
5. Complete the acquisition slice with fresh-profile creation behind the
   now-executable verified-URL, Thirtyfour,
   browser-capability, public pacing/scroll, and expected-timezone ports. Public
   identity resolution, navigation, redirect rejection, common article
   extraction, and bounded public-page pacing/scroll execution are already
   executable. Authenticated request pacing is also wired through the shared
   WeRead/article-page controller.
6. Implement synchronization, RSS publication, and HTTP/UI boundaries, followed
   by multi-replica fencing, DST, redirect-isolation, cache-stampede, and
   end-to-end tests.

Each phase must leave formatting, unit tests, Clippy, migrations, and applicable
PostgreSQL integration tests green before the next phase begins.

## Current implementation scope

The implemented foundation includes the pure pacing and quiet-hours policy in
`src/domain/pacing.rs`, the environment-only typed target-configuration loader
in `src/config.rs`, SQLx PostgreSQL pool/migration helpers in
`src/persistence/postgres.rs`, and the domain plus PostgreSQL/in-memory job
repositories in `src/domain/job.rs` and
`src/persistence/repositories/job_repository.rs`. The job repository supports
durable claim leases, fencing, retries, non-failure deferral, cancellation,
recovery, separate claim/failure counters, active-job deduplication, and
allowed-kind claiming. Its
`0001_jobs.sql` schema uses the final initial job contract, while PostgreSQL
job and account-lease decisions use statement-local `clock_timestamp()`. The
job persistence boundary also exposes the independently usable `JobQueue` and
`ExpiredJobRecovery` ports plus the command-shaped `JobOutcomeTransaction`
adapter. `UnitOfWork::job_outcomes()` hides the transaction implementation and
keeps outcome application inside the shared commit boundary; `Worker` can now
use `UnitOfWorkFactory` directly as its outcome factory, so the worker's
fenced job completion is committed by that shared boundary. This is still an
outcome-only adapter: synchronization handlers must add their article/source/
sync-run/cache writes before the one commit. The older all-in-one job traits
remain only as a migration bridge. The
pacing and configuration modules have no network, browser, database, scheduler,
or sleeping side effects. `UnitOfWorkFactory` and its transaction-scoped job
view are implemented in `src/persistence/unit_of_work.rs`; the account-lease
repository is implemented in
`src/persistence/repositories/account_lease_repository.rs`. The source
revision/feed-cache reader and transaction-scoped fenced publication are
implemented in `src/persistence/repositories/feed_cache_repository.rs`, and
`UnitOfWork` exposes that publication view. Source identity/create/read and
transaction-scoped scheduling/gate/revision mutations are implemented in
`src/persistence/repositories/source_repository.rs`. `SourceService` composes
source creation/read, operator gate changes, and atomic initial source-sync
enqueueing in `src/application/source_service.rs`. The atomic source scheduler
repository and its scheduling columns are implemented in
`src/persistence/repositories/scheduler_repository.rs`; it selects due sources
with PostgreSQL row locking, inserts canonical source-sync jobs, and records
short reservations in one transaction. Normalized article and sync-run domain
values and their PostgreSQL repositories/transaction views are implemented in
`src/domain/article.rs`, `src/persistence/repositories/article_repository.rs`,
`src/domain/sync.rs`, and `src/persistence/repositories/sync_run_repository.rs`;
they provide idempotent article upserts, feed-visible change detection,
source-scoped RSS ordering, typed sync outcomes, bounded counters, and safe
failure summaries. The sync-run repository's transaction-scoped `UnitOfWork`
view is also included in that implementation. `SourceService` and
`JobService` provide source lifecycle and worker queue orchestration, while
`ArchiveService` provides pure content normalization and hashing. The
database asset repository, anonymous asset fetcher, stable asset route, and
maintenance loop are executable when enabled; local/S3 storage, automatic
repair, and other repository views remain future work. The pure RSS renderer in
`src/rss/renderer.rs` is executable and produces revision-tagged cache
candidates. The cache-first `FeedService` in
`src/application/feed_service.rs` is also executable: it serves fresh rows,
rebuilds missing or expired rows on demand, honors conditional ETags, and
enqueues deduplicated retry jobs through the custom `jobs` table adapter when a
request-time rebuild cannot complete. It is wired to the public tokenized feed
route in `src/web/api.rs`; feed-token resolution itself is implemented by
`FeedTokenService`. `FeedRebuildService` reads normalized source
and article rows, renders outside a transaction, and publishes through the
revision/fence-aware feed-cache transaction. Its `rebuild_for_job` path also
completes a claimed `feed_rebuild` job in that same final unit of work on
successful finalization; `FeedRebuildJobHandler` maps active builders and
pre-publication failures to safe worker outcomes without double-completing a
successful job. `FeedService` is HTTP-wired by the public tokenized feed route;
the rebuild service is shared by the worker and the request-time feed path.

The one-pass scheduler wrapper in `src/application/scheduler.rs` now forwards
the configured quiet-hours policy to the atomic source-scheduling operation.
That repository samples PostgreSQL time and applies the policy inside its
transaction. `Scheduler::run_until_shutdown` owns polling, shutdown, and
transient-error backoff around this boundary; runtime composition still owns
role selection and metrics. When a refresh transport is injected, each pass
also selects active accounts nearing expiry and inserts a deduplicated
`credential_refresh` job; this maintenance pass is not suppressed by source
quiet hours. It must call this boundary rather than reimplement source or
account selection. `RuntimeSupervisor` composes the scheduler and source-sync
worker before an account is configured, and dispatches feed-rebuild,
source-sync, and transport-backed credential-refresh jobs through one
type-aware worker handler. A source-sync job with no usable account records a
warning, completes as a scheduled failure, and is reconsidered on the source's
next due interval; later admin-panel enrollment is therefore observed without
a restart.

The QR login and credential-exchange boundary is executable. Durable
credential persistence and non-interactive refresh are also executable, and the
authenticated admin panel can provision accounts through CSRF-protected routes
while returning only non-secret status metadata. Active
accounts can be scheduled for refresh when a transport is injected. Concrete
source-sync acquisition/runtime composition uses an admin-enrolled encrypted
cookie header. Binary asset persistence and URL rewriting use the optional
database backend; local/S3 storage and automatic repair remain
documentation-only. The pure archive sanitizer and `ArchiveService` are
executable. Acquisition now
contains executable identity resolution, capability/session ports, local
capacity/lease ownership, public WebDriver navigation, common article
extraction, bounded public-page pacing/scroll execution, expected
browser-timezone validation, browser-sidecar health/readiness monitoring, and
pure current/legacy WeRead article-list response parsing, authenticated
transport, account leasing, authenticated request pacing, public article
handoff, and the bounded WeRead QR transport and attempt manager are
executable. QR attempt state remains process-local until durable encrypted
attempt storage is added.
`SourceService` implements source create/read, operator enable/gate changes,
and the initial-job slice described above. `JobService` implements queue
lifecycle and transaction-scoped outcome binding; `Worker::run_once` implements
one-pass claim, lease heartbeat, handler dispatch, and fenced outcome commit,
including the shared `UnitOfWorkFactory` outcome path. `Worker::run_until_shutdown`
adds bounded idle polling, transient-error
backoff, and graceful shutdown between passes. Article and sync-run persistence
and `FeedService` implement the database/cache boundaries described above.
`AuthService` implements encrypted credential provisioning and
lease-serialized, optimistic-version refresh checks; `CredentialRepository`
stores ciphertext only, and refresh failures leave the prior version intact.
The QR login manager consumes a confirmed attempt before persistence, so a
failed account write cannot reuse the upstream session. Its status boundary
never serializes the QR payload or credential material.
`SyncService` implements the pure acquisition-result merge, archive
normalization, and typed failure classification used by the executable
source-sync handler in `src/application/source_sync_handler.rs` and its
browser-backed runtime acquirer. That handler allocates observation
versions before public-page acquisition and commits article upserts, source
scheduling/gates, sync-run completion, optional feed-rebuild enqueueing, and
fenced job outcomes through one `UnitOfWork`. QR login is provided at the
admin boundary and is separate from source synchronization. A valid public
page may omit its publication timestamp;
the service prefers the page value, then the authenticated list value, and
rejects the observation only when both are absent. Malformed WeRead article
identity or URL data is a permanent data failure; `blocked` is reserved for
verification pages and unsafe navigation results.
`TODO(design)` markers identify existing code and migrations that must change
before the remaining contracts are implemented.
