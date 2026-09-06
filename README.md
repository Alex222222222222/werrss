# Werrss

Werrss is a learning and experimentation project for exploring asynchronous
RSS generation, durable synchronization, and browser-based acquisition
boundaries.

## Disclaimer

This project is provided for learning and research purposes only. It is not
intended for production use, unauthorized access, or bypassing authentication,
rate limits, anti-abuse systems, CAPTCHAs, environment checks, or other access
controls.

If you use, modify, or deploy this project, you are solely responsible for:

- obtaining permission to access and process the content and accounts involved;
- complying with applicable laws, copyright and privacy requirements, and the
  terms and policies of every service you access;
- protecting credentials, session data, personal information, and archived
  content; and
- applying appropriate rate limits, security controls, monitoring, and data
  retention policies.

Do not use this project to collect, redistribute, or expose content or personal
data without authorization. The authors and contributors make no warranties
about availability, accuracy, legality, or fitness for a particular purpose and
are not responsible for misuse, data loss, service disruption, account action,
or any other consequence arising from its use. This project is not affiliated
with or endorsed by any third-party service it may interact with.

Review the project documentation and the applicable third-party policies before
running any component.

## First usable version scope

The first usable version includes a small authenticated admin panel for source
management, manual and QR-code WeRead credential enrollment, synchronization
status, feed-link copying, and operator-facing error states. It has one administrator
whose username and password are supplied
through `ADMIN_USERNAME` and `ADMIN_PASSWORD`; there is no user-management
lifecycle in the first version. The panel uses the same API and
application-service boundaries as other clients and does not expose credentials,
session tokens, or browser controls. The panel is implemented when
administration is explicitly enabled. The release target also
includes a reproducible Docker image build and the deployment documentation
needed to run the release image. The checked-in Dockerfile and GitHub Actions
workflow build every branch and publish a release image to GHCR for `v*.*.*`
tags.

Source synchronization also queues an `article_backfill` job when an individual
article cannot be acquired or normalized. The durable repair job carries only
the source/article identity and non-secret metadata, retries transient failures
with the source's bounded attempt budget, and deduplicates active work by source
and review ID. A successful repair invalidates the source feed revision and
queues the normal feed rebuild job.

## HTTP listener configuration

The public feeds, health endpoints, and optional admin panel share one HTTP
listener. Configure its bind host and port with environment variables:

```text
HTTP_BIND=0.0.0.0
HTTP_PORT=8080
```

`HTTP_BIND` defaults to `0.0.0.0` and accepts an IP address or resolvable host
name. `HTTP_PORT` defaults to `8080` and must be between `1` and `65535`.
There is no separate host or port for the admin panel. For an IPv6 bind, use a
raw address such as `::1`; the runtime adds the required brackets when it
constructs the socket address.

## Health endpoints

`GET /api/health` is a process-only liveness check, and `GET /api/ready` checks
PostgreSQL readiness without depending on the browser sidecar. When a browser
pool is configured, `GET /api/worker/ready` reports WebDriver availability and
browser-timezone status separately. It returns `503` until the first successful
probe and whenever either component is unavailable or mismatched; browser-backed
worker jobs remain queued during those states while database-only feed rebuilds
can continue.

## Environment variable reference

Configuration is loaded once at startup from environment variables. Variables
marked **required** must be present; all other variables use the documented
default. Durations use seconds unless the name ends in `_MS`. Invalid values,
unknown names under the application-owned prefixes, and incomplete conditional
settings fail startup. Keep database URLs, passwords, tokens, and encryption
keys in a secret manager or an ignored local environment file.

### Database and process roles

| Variable | Default | Explanation |
| --- | --- | --- |
| `DATABASE_URL` | **Required** | PostgreSQL connection URL, including any SQLx-supported SSL, certificate, and authentication query parameters. It is passed through to SQLx and never logged. |
| `DATABASE_POOL_MIN_CONNECTIONS` | `1` | Minimum number of PostgreSQL connections kept in the pool. It must not exceed the maximum. |
| `DATABASE_POOL_MAX_CONNECTIONS` | `10` | Maximum number of PostgreSQL connections. It must be greater than zero. |
| `APP_ROLES` | `api` | Comma-separated roles: `api`, `scheduler`, and/or `worker`; `all` enables every role. Scheduler startup does not require an enrolled WeRead account; source-sync jobs wait for the next scheduling pass when no usable account exists. |
| `APP_INSTANCE_ID` | Generated UUID | Stable instance identity used for distributed job leases. Set a distinct value for each long-lived replica; an ID is generated for local use when omitted. |
| `HTTP_BIND` | `0.0.0.0` | Host or IP address for the shared feed, health, and optional admin listener. IPv6 addresses may be supplied without brackets. |
| `HTTP_PORT` | `8080` | Shared HTTP listener port. It must be between `1` and `65535`. |
| `LOG_LEVEL` | `warn` | Minimum log severity: `off`, `error`, `warn`, `info`, `debug`, or `trace`. Each emitted line includes a timestamp, level, module target, and event detail. |
| `APP_TIMEZONE` | `UTC` | IANA timezone used for quiet-hours evaluation and browser timezone checks. |
| `QUIET_HOURS_START` | Unset | Inclusive local quiet-hours start in `HH:MM` format. It must be set together with `QUIET_HOURS_END`. |
| `QUIET_HOURS_END` | Unset | Exclusive local quiet-hours end in `HH:MM` format. Equal start and end values are rejected. |

### Browser and WeRead acquisition

| Variable | Default | Explanation |
| --- | --- | --- |
| `WEBDRIVER_URL` | `http://webdriver:4444` | HTTP(S) URL of the internal Thirtyfour WebDriver sidecar. |
| `BROWSER_ENGINE` | `chromium` | Browser behind WebDriver: `chromium`/`chrome` or `firefox`/`firefox-esr`. |
| `BROWSER_USER_AGENT` | Browser default | Optional fixed User-Agent for controlled diagnostics. It must be non-empty, contain no control characters, and be no longer than 512 characters; keep it consistent with the selected browser. |
| `BROWSER_LOCALE` | `zh-CN` | Locale passed to the browser profile. It must be a non-empty locale token without whitespace. |
| `BROWSER_VIEWPORT_WIDTH` | `1280` | Browser viewport width in CSS pixels. Valid range: `1`–`8192`. |
| `BROWSER_VIEWPORT_HEIGHT` | `2000` | Browser viewport height in CSS pixels. Valid range: `1`–`8192`. |
| `BROWSER_EXTRA_ARGS` | Empty | Optional whitespace-separated browser arguments. At most 32 arguments are accepted, each must begin with `-`, and controlled browser arguments such as User-Agent, window size, persistent-profile paths, and headless mode cannot be overridden. |
| `WEREAD_ACCOUNT_ID` | Unset | Optional stable WeRead account UUID used as the default panel-enrolled account. When unset, an unbound source-sync job randomly selects an enabled, unexpired account enrolled through the admin panel from PostgreSQL. A source-specific account ID takes precedence. |
| `WEREAD_ARTICLE_LIST_URL` | `https://weread.qq.com/api/mp/cover` | HTTPS WeRead article-list endpoint. The default `/api/mp/cover` response returns the latest article for the book; `/web/mp/articles` remains accepted for compatible deployments. Source synchronization first opens `https://weread.qq.com/web/shelf` in the same authenticated browser session, verifies it did not redirect to login, and fetches the list response as raw text. If public WeChat article extraction fails, it reacquires an account lease and uses `/web/mp/content?reviewId=...` in an authenticated session, extracting `#js_content`. Credentials, fragments, and non-default ports are rejected. |

### Workers, jobs, and leases

| Variable | Default | Explanation |
| --- | --- | --- |
| `WORKER_CONCURRENCY` | `1` | Maximum number of worker loops in this process. Valid range: `1`–`1024`; this controls upstream work independently from API replicas. |
| `JOB_POLL_SECONDS` | `30` | Scheduler/worker polling interval for due work and expired-job recovery. It must be positive. |
| `JOB_LEASE_SECONDS` | `600` | Duration of a claimed job lease. It must exceed `JOB_HEARTBEAT_SECONDS` plus the maximum page-operation duration. |
| `JOB_HEARTBEAT_SECONDS` | `60` | Maximum interval between job-lease heartbeats. It must be less than `JOB_LEASE_SECONDS`. |
| `JOB_MAX_ATTEMPTS` | `3` | Failure-attempt budget for retryable jobs. It must be positive. |
| `ACCOUNT_LEASE_SECONDS` | `600` | Duration of an authenticated WeRead account lease. It must exceed its heartbeat interval. |
| `ACCOUNT_HEARTBEAT_SECONDS` | `60` | Maximum interval between authenticated-account lease heartbeats. It must be less than `ACCOUNT_LEASE_SECONDS`. |
| `SOURCE_FAILURE_COOLDOWN_SECONDS` | `300` | Delay before an ordinarily failed source may be scheduled again. It may be zero and is capped at seven days. |

The interval between successful fetches of an individual source is not a
process environment variable. It is the source's `sync_interval_seconds`
setting in the admin API and defaults to one hour for a newly created source.
The next fetch is scheduled relative to the completion time of the previous
sync.

#### Job recovery

Jobs use a durable PostgreSQL lease rather than a hard execution timeout.
`JOB_LEASE_SECONDS` (default `600`) is renewed by the worker every
`JOB_HEARTBEAT_SECONDS` (default `60`). If a worker crashes, its `running` job
becomes eligible for recovery after the lease expires. Recovery is designed to
increment the failure count and requeue the job, or mark it failed after
`JOB_MAX_ATTEMPTS` is exhausted; the lease token also prevents a stale worker
from completing a job after another worker takes it over.

The production worker performs a bounded `JobService::recover_expired` pass
before each claim. Because `claim_next` only selects queued, retry-waiting, or
deferred jobs, this pass is what makes an expired `running` row claimable again.
PostgreSQL uses row locks and `SKIP LOCKED`, so concurrent worker replicas can
recover the same queue without applying the transition twice. Heartbeat-active
jobs have no maximum execution duration and may run indefinitely by design.

### RSS and feed-cache behavior

| Variable | Default | Explanation |
| --- | --- | --- |
| `RSS_CACHE_TTL_SECONDS` | `1800` | Freshness period for a persisted RSS document. It must be positive. |
| `RSS_STALE_WHILE_REVALIDATE_SECONDS` | `60` | Stale-cache fallback window advertised when an on-demand rebuild cannot provide fresh bytes. It may be zero and is capped at 24 hours. |
| `RSS_CACHE_MISS_WAIT_MS` | `5000` | Maximum time a feed request waits for an on-demand rebuild or another active builder before retry advice is returned. Valid range: `1`–`60000` milliseconds. |
| `SERVER_ROOT_URL` | Unset | Public HTTP(S) root URL used to build generated RSS channel links and absolute feed links shown by the admin panel. It is required when the API or worker role is enabled. |
| `FEED_BUILD_LEASE_SECONDS` | `600` | Duration of a distributed feed-build lease. It must exceed its heartbeat interval. |
| `FEED_BUILD_HEARTBEAT_SECONDS` | `60` | Maximum interval between feed-build lease heartbeats. It must be less than `FEED_BUILD_LEASE_SECONDS`. |

### Request pacing and bounded scrolling

Pacing values are bounded normal-distribution parameters in milliseconds. Each
sample is clamped to its configured inclusive minimum and maximum. Every value
is finite and non-negative, each minimum must not exceed its maximum, and an
individual delay is capped at 300,000 milliseconds. A zero standard deviation
produces a constant delay. These settings reduce upstream request pressure and
allow lazy page content to settle; they are not an anti-detection mechanism.

| Variable | Default | Explanation |
| --- | --- | --- |
| `PACING_REQUEST_MEAN_MS` | `2000` | Mean delay before a WeRead or other upstream protocol request. |
| `PACING_REQUEST_STDDEV_MS` | `250` | Standard deviation for upstream-request delay sampling. |
| `PACING_REQUEST_MIN_MS` | `1000` | Inclusive lower bound for upstream-request delays. |
| `PACING_REQUEST_MAX_MS` | `4000` | Inclusive upper bound for upstream-request delays. |
| `PACING_PAGE_NAVIGATION_MEAN_MS` | `3000` | Mean delay before navigating to a public article page. |
| `PACING_PAGE_NAVIGATION_STDDEV_MS` | `500` | Standard deviation for page-navigation delay sampling. |
| `PACING_PAGE_NAVIGATION_MIN_MS` | `1500` | Inclusive lower bound for page-navigation delays. |
| `PACING_PAGE_NAVIGATION_MAX_MS` | `7000` | Inclusive upper bound for page-navigation delays. |
| `PACING_PAGE_ACTION_MEAN_MS` | `1000` | Mean delay between public page actions such as extraction and scrolling. |
| `PACING_PAGE_ACTION_STDDEV_MS` | `200` | Standard deviation for page-action delay sampling. |
| `PACING_PAGE_ACTION_MIN_MS` | `500` | Inclusive lower bound for page-action delays. |
| `PACING_PAGE_ACTION_MAX_MS` | `3000` | Inclusive upper bound for page-action delays. |
| `PACING_SCROLL_SETTLE_MEAN_MS` | `1000` | Mean delay after a scroll so lazy-loaded content can settle. |
| `PACING_SCROLL_SETTLE_STDDEV_MS` | `200` | Standard deviation for scroll-settle delay sampling. |
| `PACING_SCROLL_SETTLE_MIN_MS` | `500` | Inclusive lower bound for scroll-settle delays. |
| `PACING_SCROLL_SETTLE_MAX_MS` | `3000` | Inclusive upper bound for scroll-settle delays. |
| `SCROLL_MAX_STEPS` | `4` | Maximum number of bounded scroll actions per article page. Valid range: `1`–`64`. |
| `SCROLL_MAX_PIXELS` | `4000` | Maximum cumulative CSS-pixel scroll distance per article page. Valid range: `1`–`1000000`. |
| `SCROLL_MAX_OPERATION_SECONDS` | `30` | Maximum duration of one page interaction and scroll operation. It must be positive and no greater than one hour. |

### Asset caching

The detailed, implementation-neutral contract is in
[docs/ASSET_CACHING.md](docs/ASSET_CACHING.md). Asset caching is disabled by
default. With the `database` backend, the worker fetches approved article
images using a separate anonymous HTTP request with the article `Referer`,
derived `Origin`, and configured User-Agent, validates the media type and
signature, stores/deduplicates the bytes in PostgreSQL, and rewrites the
article HTML to stable `/assets/{id}` paths. WeChat/WeRead cookies are never
sent to asset hosts. Missing cached bytes enqueue a deduplicated anonymous
repair job and retain the same asset URL while the worker restores the data.
Local-directory and S3 backends remain future work.

| Variable | Default | Explanation |
| --- | --- | --- |
| `ASSET_ARCHIVE_BACKEND` | `disabled` | `disabled` keeps approved external URLs. `database` (or `postgres`) stores bytes and URL/version metadata in PostgreSQL. `local` and `s3` are not implemented and are rejected. |
| `ASSET_CACHE_MAX_SIZE_MB` | `5000` | Aggregate limit for cached raw binary bytes, in decimal megabytes. `0` disables size-based eviction. |
| `ASSET_CACHE_MAX_AGE_DAYS` | `30` | Maximum idle age since the last successful asset read before bytes are evicted. `0` disables age-based eviction. |
| `ASSET_MAX_SIZE_MB` | `10` | Maximum size of one downloaded asset, in decimal megabytes. It is always enforced. |
| `ASSET_MAX_COUNT_PER_ARTICLE` | `100` | Maximum number of assets fetched while processing one article. Excess assets remain external. |
| `ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB` | `100` | Aggregate response-byte budget for one article, in decimal megabytes. Excess assets remain external. |
| `ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS` | `120` | Wall-clock budget for fetching assets for one article. Excess assets remain external. |
| `ASSET_FETCH_TIMEOUT_SECONDS` | `30` | Timeout for one asset request, including redirects and body transfer. |
| `ASSET_MAX_REDIRECTS` | `5` | Maximum number of redirects followed for one asset request. |
| `ASSET_REPAIR_MAX_PENDING` | `100` | PostgreSQL-wide maximum active asset-repair jobs. |
| `ASSET_REPAIR_PUBLIC_MAX_PENDING` | `50` | PostgreSQL-wide maximum active repairs admitted by public asset requests; `0` disables public admission. |
| `ASSET_REPAIR_REQUEUE_SECONDS` | `60` | Backoff after a failed repair before another public admission. |
| `ASSET_REPAIR_MAX_ATTEMPTS` | `3` | Maximum repair admissions for one stable asset record before it becomes terminal. |

The database backend runs a best-effort maintenance pass hourly in the runtime
supervisor. Age and size eviction clear only binary data and mark the retained
URL/version metadata as `missing`; orphan cleanup removes records and blobs
that no longer have an article relationship. A missing referenced asset returns
`503` with `Retry-After` set to `ASSET_REPAIR_REQUEUE_SECONDS` after admitting
one deduplicated `asset_repair` job. Unknown, unreferenced, or exhausted
records return `404`.

### Administration and encryption

| Variable | Default | Explanation |
| --- | --- | --- |
| `ADMIN_ENABLED` | `false` | Enables the single-administrator API and panel when `true`. |
| `ADMIN_USERNAME` | Unset | Required only when `ADMIN_ENABLED=true`; configured administrator username. |
| `ADMIN_PASSWORD` | Unset | Required only when `ADMIN_ENABLED=true`; configured administrator password. Store it as a secret. |
| `SESSION_SIGNING_KEY` | Unset | Required only when `ADMIN_ENABLED=true`; independent secret used to sign admin sessions. It must differ from the admin password and credential-encryption key. |
| `CREDENTIAL_ENCRYPTION_KEY` | **Required** | Secret used to encrypt WeRead credentials before persistence. It must be protected and must not be reused as the session-signing key. |

## Logging

Werrss writes structured, container-friendly logs to standard error. Set
`LOG_LEVEL` to `warn` (the default), `off`, `error`, `info`, `debug`, or `trace`
to control verbosity. Every emitted line includes a timestamp, severity,
module target, and event detail; completed instrumented operations also include
their duration when span-close logging is enabled. Third-party WebDriver
response logging remains capped at warning level even when application debug
or trace diagnostics are enabled.

The levels have these meanings:

- `error`: a role or process cannot provide its contract, or a durable
  operation failed.
- `off`: disables log output.
- `warn`: a recoverable failure, expected degradation, invalid operator input,
  or an unavailable upstream dependency.
- `info`: process and role lifecycle events and completed user or worker work.
- `debug` and `trace`: bounded control-flow, timing, counts, identifiers, and
  other diagnostics useful while investigating a single operation.

Logs never include passwords, encryption keys, session or feed tokens, cookie
values, request bodies, or raw upstream response documents. Keep the default
level in production and temporarily use `LOG_LEVEL=debug` (or `trace` for
low-level browser and pacing diagnostics) while investigating a problem.

## Roadmap

These possible features are ordered approximately by user value and operational
impact, not by implementation difficulty. The list is a planning guide rather
than a commitment:

Structured application logging, sensitive-value redaction, browser health and
worker-readiness diagnostics, and missed-article repair/backfill jobs are
included in the current runtime. The responsive administrator UI polish and the
English, French, and Simplified Chinese panel translations are also included in
the current panel. The remaining roadmap items are future work:

1. **Add local-directory and S3-compatible asset backends.** These can make
   binary storage easier to operate at larger scale while preserving the
   database metadata and stable asset IDs.
2. **Evaluate PGMQ as a queue transport optimization.** The current custom
   `jobs` table remains the version-one transport; PGMQ can be evaluated later
   if queue throughput or operational overhead becomes a demonstrated
   bottleneck.

The release image is built by `.github/workflows/container.yml`. Push a
semantic-version tag such as `v0.1.12` to build and publish
`ghcr.io/<owner>/<repository>:v0.1.12` and `:latest`; branch and pull-request
builds validate the Dockerfile without publishing. The image expects the same
environment variables described in [DEPLOYMENT.md](DEPLOYMENT.md), including
`DATABASE_URL`; no credentials are baked into the image. The Dockerfile keeps
dependency downloads and build artifacts in BuildKit cache mounts and uses a
small non-root distroless runtime image. The application image does not need
`tzdata`; the browser sidecar still does.

## WeRead authentication

Authentication is split into two separate flows. Both flows must use an
account-specific distributed lease, and neither flow may expose access or
refresh tokens in logs, API responses, job payloads, or error messages.

### Non-interactive provisioning and refresh

The implemented authentication lifecycle accepts credentials from a trusted
operator or another approved login adapter through `AuthService::provision`.
Credentials are encrypted with the configured `CREDENTIAL_ENCRYPTION_KEY`
before being written to PostgreSQL. The `weread_accounts` table stores only
encrypted credential material, account metadata, expiry, and an optimistic
credential version.

`AuthService::refresh_if_needed` samples the repository's authoritative clock,
checks the configured refresh window, and refreshes only when the access
credential is near expiry. Refresh is serialized with the existing account
lease; the lease is heartbeated while the exchange runs, and a lease-fenced,
version-checked replacement prevents stale writes. A refresh response may
rotate the refresh token. If it does not, the previous refresh token is
retained. Authentication-required and risk-control results stop the operation
and require operator action; they are not retried automatically.

This service boundary is implemented and tested. Administrators can enroll an
already-issued WeRead web cookie header from `/admin`; the API accepts it only
on the protected, CSRF-guarded `POST /api/admin/weread/accounts` route and
returns only the generated account ID and non-secret status metadata. `GET
/api/admin/weread/accounts/{account_id}` returns that same metadata for
checking an enrollment. Re-authentication uses the protected `PUT
/api/admin/weread/accounts/{account_id}` route and keeps the same account ID
for existing source references. Credentials cannot be read back through the
panel. The QR flow is available from the same panel: it provisions a new or
selected account only after the administrator confirms the short-lived code,
and keeps the temporary QR attempt and upstream credentials out of status
responses.

A deployment-specific
`CredentialRefresher` can be injected into `RuntimeSupervisor`; that enables
`credential_refresh` jobs in the worker plan and makes the runtime scheduler
enqueue one deduplicated refresh job for each active account within the
refresh window. The browser-backed source-sync runtime always loads the
enrolled cookie header through `AuthService` and injects it into a fresh
authenticated browser session. See
[`AuthService`](src/application/auth_service.rs) and the
[authentication architecture](ARCHITECTURE.md#source-scheduling-and-account-leases).

For each article, source synchronization first tries the public WeChat URL in
a clean browser session. When that fetch fails, the worker reacquires the
selected account lease, opens the WeRead shelf, and visits
`/web/mp/content?reviewId=...` in the authenticated session. The rendered page
source is parsed from `#js_content`; title and publication time may fall back
to metadata from the authenticated list response. The public URL is still
required as the canonical RSS item URL, and the authenticated session is
never used to visit a public WeChat page.

Scheduler and worker roles can start before any WeRead account is enrolled.
When a due source-sync job finds no enabled, unexpired account, it records a
warning and a scheduled failure; the source is reconsidered on its next due
interval. Enrolling or replacing credentials in the admin panel makes the
account available to later jobs without restarting the worker.

When adding a source, its WeRead account ID is optional. Set the ID returned by
the admin panel to pin that source to one account; leave it empty to randomly
select among enabled, unexpired enrolled accounts for each synchronization
job.

For a shelf-first authenticated browser diagnostic against a temporary
WebDriver sidecar, see the
[deployment diagnostic flow](DEPLOYMENT.md#authenticated-browser-diagnostic).

### Single-admin panel and API

Enable the first-version administration surface with `ADMIN_ENABLED=true`,
`ADMIN_USERNAME`, `ADMIN_PASSWORD`, and an independent `SESSION_SIGNING_KEY`.
Open `/admin/login` and submit the configured credentials. A successful
`POST /api/admin/login` returns a short-lived `HttpOnly` session cookie and a
CSRF token; send that token as `X-CSRF-Token` on every state-changing admin
request. The panel is available at `/admin` and uses these API routes:

- `GET /api/admin/sources` and `POST /api/admin/sources` for source management;
- `GET /api/admin/sources/{id}` to inspect a source and `PUT`/`DELETE` on the
  same path to edit or remove it; the panel exposes this at
  `/admin/sources/{id}`;
- `POST /api/admin/sources/{id}/enabled` and `/gate` for operator controls;
- `POST /api/admin/sources/{id}/feed-token` to create/rotate a copyable feed
  link. The response always includes `feed_path` and `feed_url`; `feed_url` is
  absolute when `SERVER_ROOT_URL` is configured and `null` otherwise, and is
  the value displayed by the panel; and
- `GET /api/admin/sources/{id}/sync-runs` for synchronization history;
- `POST /api/admin/weread/accounts` to enroll an already-issued WeRead cookie
  header and expiry; and
- `PUT /api/admin/weread/accounts/{account_id}` to replace an account's cookie
  header without changing source references; and
- `GET /api/admin/weread/accounts/{account_id}` to inspect non-secret account
  status.

Sources can be created with either a `book_id` or an `article_url` (or both).
When only an article URL is supplied, the API resolves its WeRead book ID from
the long URL or through the configured clean browser sidecar for a short
`/s/...` link. A supplied book ID always wins. The display name is optional:
the resolved public-account name is preferred, then the book ID is used as a
stable fallback. An article URL is stored when supplied; book-only sources
store no URL. `account_id` is also optional and, when present, pins future
source synchronization to that WeRead account; otherwise the worker selects
an enabled, unexpired account at run time.

There is no user-management endpoint. Put the application behind TLS in a
deployment so the session cookie and credentials are protected in transit.

### Panel language

The admin panel supports English (`en`), French (`fr`), and Simplified Chinese
(`zh`; `ch` is accepted as an alias when parsing a language preference). It
uses the saved `werrss_locale` cookie first, then the browser's
`Accept-Language` header, and falls back to English. Use the language selector
in the login page or any authenticated admin page to save a preference for
future visits. User-supplied account names, source names, and API error detail
are kept separate from the static translation catalog.

### QR login

The admin panel includes an interactive QR login flow for enrolling a new or
existing WeRead account without copying a cookie header. It follows a bounded,
single-use lifecycle:

1. create a short-lived, single-use login attempt bound to one account;
2. display the server-generated QR SVG without logging its value;
3. poll with a bounded interval and deadline, handling pending, scanned,
   confirmed, expired, cancelled, and risk-controlled states;
4. exchange the confirmed result through the WeRead QR transport; and
5. validate the account identity, encrypt the credentials, provision the
   account, and discard the temporary login state.

The routes are `POST /api/admin/weread/qr` to start, `GET
/api/admin/weread/qr/{attempt_id}` to poll, and `DELETE
/api/admin/weread/qr/{attempt_id}` to cancel. All three routes require
administrator authentication and CSRF protection because a successful poll
finalizes account persistence. Attempt creation is bounded, expires after five
minutes by default, and successful attempts are consumed before account
persistence. QR contents and upstream credentials are never logged, persisted
as attempt state, or returned in status responses.

The attempt manager is process-local. With more than one API replica, route the
start, poll, and cancel requests for one attempt to the same replica until
durable encrypted attempt storage is added. The real QR scan remains an opt-in
manual test; automated tests cover state transitions, expiry, cancellation,
response redaction, and admin provisioning.

## Test coverage

Generate coverage locally with `cargo-llvm-cov`. The reports are build
artifacts and are intentionally written under `target/`, not committed to the
repository.

Install the tool and its Rust support component once:

```sh
cargo install cargo-llvm-cov --locked
rustup component add llvm-tools-preview
```

Run the database-independent unit-test coverage report with missing lines:

```sh
cargo llvm-cov --all-features --workspace --lib --text --show-missing-lines
```

Generate an HTML report for interactive inspection:

```sh
cargo llvm-cov --all-features --workspace --lib --html
# Open target/llvm-cov/html/index.html in a browser.
```

Export an LCOV report for CI or other coverage tooling:

```sh
cargo llvm-cov --all-features --workspace --lib \
  --lcov --summary-only --output-path target/llvm-cov/unit.lcov
```

To include PostgreSQL integration tests, set `DATABASE_URL` to an
administrative PostgreSQL connection that the SQLx test harness may use, then
run the integration targets:

```sh
: "${DATABASE_URL:?set DATABASE_URL before running database coverage}"
cargo llvm-cov --all-features --workspace --tests --html \
  --output-dir target/llvm-cov/integration-html
```

The integration command may create isolated databases for each
`#[sqlx::test]`; use the same external development database and resource
limits documented for the nextest workflow. Keep credentials in the shell or
an ignored local development file, never in README examples or committed
reports.

For the complete test suite, install `cargo-nextest` and use the repository's
checked-in `.config/nextest.toml` configuration:

```sh
cargo install cargo-nextest --locked
cargo nextest run --locked --tests -j 16
```

The global `-j 16` keeps database-independent tests fast, while the
configuration places the API and PostgreSQL-backed test binaries in a group
limited to sixteen concurrent isolated-database setups. This keeps the
database workload bounded independently if the global test concurrency is
raised later. The cap can be lowered in `.config/nextest.toml` for a smaller
or shared PostgreSQL service.
