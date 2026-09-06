# Asset caching design

Status: the disabled/database first slice is implemented. The current runtime
accepts the database policy, fetches approved public image assets before the
article persistence transaction, stores them in PostgreSQL, rewrites successful
references to `/assets/{asset_record_id}`, serves stable asset responses,
admits durable missing-asset repair jobs, and runs hourly best-effort
maintenance. The default `disabled` mode performs no asset network or database
writes and keeps approved external image URLs.

Automatic repair of a missing database-backed asset is implemented. Local
directory storage, S3 storage, and signed-URL refresh lineages remain future
work. The repair implementation covers only the public, anonymous re-fetch
path and does not infer that an HTTP error means a signed URL should be
refreshed.

## Goals

Asset caching makes an archived article useful when an upstream image is slow,
unavailable, or later replaced. It has four separate responsibilities:

1. identify an asset URL and the article references that use it;
2. fetch and validate the binary using the same request context as the article
   browser session;
3. deduplicate and store the bytes; and
4. serve, expire, and recover the cached bytes without deleting useful URL
   metadata.

The first enabled storage implementation is PostgreSQL. The storage interface
must remain independent of PostgreSQL so a local-directory or S3-compatible
backend can be added later, but neither of those backends is part of the first
asset-cache implementation.

Asset caching is best effort. An article remains persistable when one or more
asset downloads fail. The article can retain its external URL until an asset
is successfully stored and its feed representation is rebuilt.

## Configuration contract

The canonical backend name is `database`. It means that binary data is stored
in PostgreSQL. `postgres` is accepted as a compatibility alias for the same
backend, not as a second behavior. For the first asset-cache implementation,
`ASSET_ARCHIVE_BACKEND` accepts `disabled`, `database`, and `postgres`; `local`
and `s3` are reserved future values and must not be accepted until their
storage implementations exist.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ASSET_ARCHIVE_BACKEND` | `disabled` | `disabled` leaves external URLs unchanged; `database` stores bytes in PostgreSQL. `local` and `s3` are future backends and are not part of the first implementation. |
| `ASSET_CACHE_MAX_SIZE_MB` | `5000` | Maximum aggregate size of cached binary bytes. `0` disables size-based eviction. The value is decimal megabytes: `5000` means 5,000,000,000 bytes. |
| `ASSET_CACHE_MAX_AGE_DAYS` | `30` | Maximum age since the last successful access before cached bytes are evicted. `0` disables age-based eviction. |
| `ASSET_MAX_SIZE_MB` | `10` | Maximum size of one downloaded asset, in decimal megabytes. This is always enforced and must be greater than zero. |
| `ASSET_MAX_COUNT_PER_ARTICLE` | `100` | Maximum number of distinct assets fetched while processing one article. Excess assets remain external. |
| `ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB` | `100` | Maximum aggregate response bytes fetched for one article, in decimal megabytes. Excess assets remain external. |
| `ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS` | `120` | Maximum wall-clock time spent fetching assets for one article. Excess assets remain external. |
| `ASSET_FETCH_TIMEOUT_SECONDS` | `30` | Maximum time for one asset request, including redirects and body transfer. |
| `ASSET_MAX_REDIRECTS` | `5` | Maximum number of redirects followed for one asset request. |
| `ASSET_REPAIR_MAX_PENDING` | `100` | PostgreSQL-wide maximum number of active asset-repair jobs. |
| `ASSET_REPAIR_PUBLIC_MAX_PENDING` | `50` | PostgreSQL-wide maximum number of active repairs admitted by public asset requests. `0` disables public repair admission. |
| `ASSET_REPAIR_REQUEUE_SECONDS` | `60` | Backoff after a failed repair before another public admission is allowed. |
| `ASSET_REPAIR_MAX_ATTEMPTS` | `3` | Maximum number of public repair admissions for one stable asset record before it becomes terminal. |

`ASSET_REFRESH_*` settings are reserved for the future signed-URL refresh
feature and are not accepted by the current environment parser.

The aggregate cache limit counts only the raw binary response bytes for rows
whose data is present. It does not count PostgreSQL TOAST overhead, indexes,
metadata, or rows whose data has already been evicted. `0` does not disable the
per-asset limit, network safety limits, or orphan cleanup.

If a single asset is larger than `ASSET_MAX_SIZE_MB`, it is rejected without
being persisted. If the aggregate limit is smaller than that asset and no
evictable data can make room, the article remains valid and the asset remains
external; the worker must not loop indefinitely trying to cache it.

Configuration is loaded at startup. Runtime changes require a restart unless a
future settings service explicitly replaces the environment-only contract.

### Cluster-wide recovery-job admission

`ASSET_REPAIR_MAX_PENDING` is a PostgreSQL-wide limit, not a per-process hint.
The count includes every non-terminal asset recovery row in `queued`, `running`,
`retry_wait`, or `deferred` state for both `asset_repair` and `asset_refresh`.
Terminal `succeeded` and `failed` rows do not consume admission capacity. The
limit is shared by every API, scheduler, and worker replica that uses the same
database.

Every enqueue origin uses the same short PostgreSQL transaction and admission
helper. The helper must:

1. acquire one fixed PostgreSQL transaction advisory lock for the asset-recovery
   namespace;
2. check the active deduplication key first and return the existing job without
   consuming another slot when one already exists;
3. count outstanding asset-recovery jobs while holding that lock;
4. for a public `asset_repair`, require both the global count to be below
   `ASSET_REPAIR_MAX_PENDING` and the public count to be below
   `ASSET_REPAIR_PUBLIC_MAX_PENDING`;
5. for any internal repair or `asset_refresh`, require only the global count to
   be below `ASSET_REPAIR_MAX_PENDING`; and
6. insert the job and commit the typed admission decision in the same
   transaction.

The count must not use `SKIP LOCKED` or an eventually consistent process-local
counter, because either can undercount concurrent replicas. A unique active-job
deduplication constraint remains necessary, but it is not a substitute for the
global capacity check. `ASSET_REPAIR_PUBLIC_MAX_PENDING` must be no greater than
`ASSET_REPAIR_MAX_PENDING`; a value of zero disables public repair admission and
leaves all global capacity for internal work.

When the global or public class limit is full, the public route returns its
normal retryable response with `Retry-After` and creates no row. If an internal
refresh cannot be admitted, its triggering repair job enters the existing
non-failure `deferred` state with a bounded `run_after`; it must not use
`retry_wait`, because that transition consumes the job failure budget. The
deferred job records a bounded capacity-wait reason and, when it wakes, retries
admission before performing another upstream fetch. The refresh lineage attempt
is not consumed until the refresh job is actually admitted. This preserves the
old stable asset route without creating an unbounded local queue. Database
errors likewise create no local fallback queue: the request or worker records a
bounded warning and the triggering work uses the same deferred capacity-wait
path when it is safe to retry.

The capacity-wait transition and refresh admission must be committed atomically
with the triggering job's lease-fenced outcome. A worker crash before that
transaction commits leaves the original job recoverable; a crash after it
commits leaves either the durable deferred trigger or the admitted refresh, but
never neither.

The generic `jobs` table must retain an immutable asset-recovery admission
class, either as a constrained column or in a one-to-one child table joined by
`job_id`. The class is `public_repair` for a cache-miss HTTP request,
`internal_repair` for any future scheduled repair, and `internal_refresh` for a
signed-URL refresh. It is absent for other job kinds. The class is assigned by
the enqueue path, is not taken from untrusted JSON payload, and is never changed
when a job is claimed or retried. Public-cap queries count only
`public_repair`; the global-cap query counts all three classes. This makes the
quota enforceable when multiple enqueue origins share a job type.

## Identity and database model

The design deliberately separates a URL, a version of an asset observed at
that URL, the shared bytes, and an article relationship. A suggested logical
model is:

### `asset_blobs`

One row represents one unique byte sequence that may be shared by many URL
records and articles.

- `id`: opaque stable identifier used internally;
- `checksum_algorithm` and `checksum`: currently SHA-256 and its digest;
- `byte_size`: exact raw response length;
- `media_type`: validated media type used when serving;
- `data`: PostgreSQL binary data, nullable after eviction;
- `created_at`, `last_fetched_at`, and `last_accessed_at`.

The blob ID must not be only the checksum. A checksum match is a candidate for
deduplication, not proof that bytes match. The implementation compares raw
bytes after matching checksum and size. If the bytes differ, it creates a
separate blob and never overwrites the existing one.

### `asset_records`

One row represents an exact source URL observation/version and retains enough
metadata to retry it after the data has been evicted.

- `id`: opaque ID used by the stable `/assets/{id}` route;
- `source_url`: the exact URL, excluding only the non-transmitted fragment;
- `final_url`: the validated URL after redirects, when known;
- `blob_id`: nullable when fetching has not produced bytes; it remains set
  when only the blob data has been evicted;
- fetch status and a bounded error classification. The current schema uses
  `available` and `missing`; `asset_repair_states` stores bounded admission,
  backoff, and terminal-attempt state for public repair;
- a safe non-secret fetch-context reference when one is needed for recovery.

Do not store an unbounded list of original URLs on a blob. Multiple
`asset_records` may point to one `asset_blob`, and each source URL remains
individually auditable. Query parameters must not be stripped merely because
they look like tracking parameters: they may be signed image URLs. URL lookup
normalization may lowercase the scheme and host, remove a default port, and
remove a fragment, but it must preserve path, query, and user-visible URL
semantics.

If one exact source URL later returns different bytes, create a new
`asset_record` version rather than changing the blob used by older article
references. Older articles then retain their original stable asset URL. If a
candidate blob has already had its data evicted, it cannot be raw-byte
compared; treat it as a non-match and allow orphan cleanup to remove it later.

The stored source URL is a retry hint, not an assumption that a signed URL
remains valid forever. A refreshed article may produce a different URL for the
same image, and that URL is recorded as a new asset-record version. Older
article references continue to point at their existing version until the
article itself is successfully refreshed.

### `article_assets`

This join table connects an article to an asset record and optionally stores
the role and occurrence order, such as `cover` or `body` image. It must have
foreign keys that make article deletion remove the relationship, while not
deleting a shared blob still referenced by another article.

Every asset record that is exposed through a rewritten article must have at
least one article relationship. Once its relationship count reaches zero, its
URL metadata and blob metadata may be deleted, but only after active fetch and
serve leases have finished. A referenced record may retain its metadata even
when `blob_id` is null or its blob data is null.

The request context is logically attached to the article relationship, not
assumed to be globally identical for every use of a URL. The implementation
may store it in `article_assets` or a separate context table. It contains only
non-secret values such as the article-page URL, origin, and User-Agent profile;
the first implementation does not store or reuse cookie values for asset
fetches.

### Asset recovery job metadata

Asset recovery jobs retain their typed admission class and their asset/article
identity in durable columns or a one-to-one recovery-job table. The job payload
may repeat the non-secret identity for dispatch, but capacity accounting and
refresh-lineage updates must use the typed durable fields rather than parsing
JSON.

## Acquisition

The HTML sanitizer remains a pure operation. It reports approved absolute
HTTP(S) image URLs but does not perform network I/O.

During article acquisition, the browser opens the article page and performs
the bounded scrolling needed to trigger lazy images. For every discovered
article/asset relationship, the browser result must also provide an asset
request context containing:

- the exact article-page `Referer` for that relationship;
- the `Origin` observed or derived from that page;
- the browser User-Agent used for the session; and
- the page URL from which the asset URL was extracted.

WebDriver DOM access alone is not sufficient to recover response bodies. The
implementation therefore uses a separate bounded HTTP client request for each
asset. Because WeChat official-account pages and their image hosts are public,
the first implementation performs this request anonymously: it sends the
validated `Referer`, `Origin` when required by the request policy, and the
browser User-Agent, but never sends a WeRead `Cookie` header and never acquires
a WeRead account lease for an asset request. The account selected for the
authenticated WeRead list/content operation is not implicitly reused here.

If a future asset host genuinely requires authentication, it must be an
explicit opt-in mode outside the first implementation. An authenticated
asset-fetch/repair mode may send only
cookies whose domain, path, Secure flag, and expiry match the exact target URL;
it must never forward the complete browser cookie jar to an arbitrary asset
host. Such a future mode would acquire its account lease at execution time,
not from a public HTTP request, and its job payload would still contain no
secrets.

Each response is streamed with the configured byte limit. The client validates
the status, redirect chain, media type, and file signature before handing the
bytes to the asset store. A failed asset request records a warning and a
bounded failure classification; it does not fail the article transaction.

Asset fetching is bounded per article. The worker processes distinct URLs in
sanitizer order until `ASSET_MAX_COUNT_PER_ARTICLE`,
`ASSET_MAX_FETCH_BYTES_PER_ARTICLE_MB`, or
`ASSET_MAX_FETCH_TIME_PER_ARTICLE_SECONDS` is reached. It then leaves the
remaining URLs external and records one bounded warning for the budget that
was exhausted. The article and feed remain valid.

Source synchronization also caps successful asset bytes retained across the
whole prepared batch at 256 MiB. Once that process-memory bound is reached,
later articles keep their external URLs for that run; their asset bytes are
not retained until a later observation. This bound protects a source with many
large articles even when every individual article is within its own fetch
budget.

If an article already has a cached representation and a later observation
cannot fetch every external asset, the worker keeps the previous HTML and its
article-to-asset relationships together. It may still update the article's
other metadata. This avoids replacing cached HTML with a partial rewrite or
leaving relationship rows that no longer match the HTML. A new article, or an
article that has no cached representation, may persist the successfully
fetched subset with the remaining URLs external.

All browser and HTTP work occurs before the short article persistence
transaction. A database connection or article row lock must not be held while
waiting for a browser, upstream server, redirect, or image body.

## Deduplication on asset addition

Deduplication happens in the add/attach path. There is no later global scan or
background deduplication job.

1. Sanitize the article and obtain the exact asset URL.
2. Fetch and validate the response, calculate SHA-256 and byte size before
   persistence, and begin the asset preflight before source/article row locks
   are acquired.
3. Under the checksum lock, compare raw bytes with each present candidate.
   Reuse the matching blob, or record that a new blob is required.
4. Insert or update the URL/version record and attach it to the article in the
   same short transaction as the rewritten article HTML. The write path only
   rechecks candidate metadata and never loads large candidate bodies while
   source/article locks are held.

The current implementation serializes same-URL, same-checksum, and aggregate
capacity decisions with PostgreSQL transaction advisory locks. Two URLs with
equal bytes share one blob but retain two source records. Two requests for the
same URL and same bytes share one URL/version record. A same-URL byte change
creates a new version. A failed initial fetch leaves the article external; a
successful initial fetch always creates the referenced record used by a later
repair if its bytes are subsequently evicted.

If the process stops after writing a blob but before attaching it to an
article, the unreferenced blob is removed by orphan cleanup. If it stops after
the article transaction commits, a retry sees the existing URL/version and
does not create duplicate data.

## Rewriting and serving

When an asset is successfully added, sanitized HTML uses the stable local URL
`/assets/{asset_record_id}`. Rewriting is limited to URLs reported by the
sanitizer for that article; unrelated attributes and links are never rewritten.

The public asset route:

1. loads the asset record by opaque ID;
2. confirms that it is still referenced by an article;
3. serves present bytes with the stored validated media type, a checksum ETag,
   `X-Content-Type-Options: nosniff`, and an appropriate cache policy;
4. updates `last_accessed_at` for successful `200` and `304` responses; and
5. when data is missing, does not perform network or browser work in the
   public request. The route admits one deduplicated `asset_repair` job and
   returns `503 Service Unavailable` with `Retry-After` set to
   `ASSET_REPAIR_REQUEUE_SECONDS`; an unknown, unreferenced, or exhausted
   record returns `404`.

The public asset route never acquires an account lease, sends cookies, starts
an authenticated browser session, or follows an upstream URL synchronously.
Both the initial article worker and the `asset_repair` worker perform the same
bounded anonymous HTTP fetch using the article URL as `Referer`, its origin as
`Origin`, and the configured User-Agent.

When the stored URL is classified as expired because its signed URL is stale,
the repair worker does not retry that URL indefinitely. Refresh eligibility is
provider-specific and must not be inferred from an HTTP status alone:

- a `401` or `403` is eligible only when the approved provider policy identifies
  the response as an expired signed URL rather than an access-control or policy
  failure;
- a `404` or `410` is permanent by default and is not refreshed; a provider
  adapter may opt into refresh only with explicit evidence that the response
  means an expired signed URL; and
- invalid media, DNS/connection errors, and ordinary `5xx` responses remain
  normal repair failures and use the job failure budget rather than starting an
  article refresh.

For an eligible result, it enqueues one deduplicated `asset_refresh` job for the
live article relationship. Admission reserves one refresh attempt in the
relationship's durable refresh lineage in the same transaction as the job
insert. The refresh worker reopens the public article page in a clean,
unauthenticated browser session, extracts the current sanitized asset URL, and
runs the normal add-time validation and deduplication path. It never uses
WeRead account cookies for this public page or asset request.

Each article/asset occurrence has a durable refresh-lineage state, stored on the
relationship or in a table keyed by the article, occurrence, and lineage ID.
The state includes the attempt count, `next_allowed_at`, the configured maximum,
the last bounded failure classification, and an exhausted/resolved marker. The
lineage ID survives replacement of an `asset_record`; the refresh transaction
updates the existing occurrence state or carries it forward when it attaches the
replacement record. A refresh therefore cannot evade the limit merely by
discovering a new signed URL. Retries of one admitted job do
not consume additional lineage attempts; a newly admitted refresh does. A
successful refresh that stores usable bytes marks the lineage resolved and
resets its attempt count for a later, genuinely new expiry. A failed or
inconclusive admitted refresh consumes one attempt and observes
`ASSET_REFRESH_COOLDOWN_SECONDS`.

If refresh finds a new URL or new bytes, it creates a new asset-record version,
attaches it to the article, rewrites the article representation, and queues a
feed rebuild. If it finds the same bytes, it may restore the existing blob and
update the source metadata without creating duplicate binary data. If the
article or asset remains unavailable, the old metadata and stable asset URL
remain intact. While a repair or refresh is eligible, the public route returns
its retryable response; after a permanent classification or an exhausted
refresh lineage, it returns a terminal missing-asset response without starting
more upstream work. The original URL is never exposed as an unrestricted
redirect target.

A successful repair that restores the same stable asset record does not require
a feed rebuild. A new URL/version or the first successful rewrite does
invalidate the source feed revision and queues a feed rebuild.

`asset_repair` and the future `asset_refresh` are separate job kinds. Repair
jobs are deduplicated by stable asset identity, use the normal durable lease
and crash-recovery rules, and share the cluster-wide pending limit and
per-process concurrency limit. Repair jobs use `ASSET_REPAIR_REQUEUE_SECONDS`;
future refresh admission uses
the refresh-lineage cooldown and attempt limit. A repair may enqueue at most one
refresh for the current lineage at a time; a refresh must not recursively
enqueue another refresh without first completing a new article acquisition.

The cache-miss enqueue path rejects unknown or unreferenced IDs without
creating work. Repeated requests for the same missing ID do not create more
than one active job, and once the cluster-wide or public-class pending limit is
reached they return the same retryable response without growing the queue. A
per-asset `ASSET_REPAIR_REQUEUE_SECONDS` backoff prevents request traffic from
turning a permanent upstream failure into a hot retry loop. Public admission
cannot consume the capacity reserved for internally generated refresh jobs.
Before enqueuing, the route also checks the durable repair state. If the
asset-repair lineage is terminal or exhausted, it returns the terminal
missing-asset response and creates no job, regardless of how much time has
passed since the last request. A permanent `404`/`410` sets that marker on the
first classified failure; retryable failures consume one admission from
`ASSET_REPAIR_MAX_ATTEMPTS` and set it when the budget is exhausted. A
successful repair clears the marker. Only a successful normal article
acquisition that produces a new asset observation or fresh usable bytes may
clear a terminal state without a successful repair; merely persisting the same
article while its asset fetch fails does not. A public cache miss therefore
cannot reset the circuit breaker.

## Eviction and maintenance

Eviction is a scheduled, bounded maintenance job and is not performed in an
RSS request.

### Age eviction

For each referenced blob whose data is present, if
`last_accessed_at + ASSET_CACHE_MAX_AGE_DAYS` is earlier than the PostgreSQL
current time, the job sets only `asset_blobs.data` to `NULL`. It retains the
blob metadata, source URL, and article relationships. The initial access time
is the successful store time, so an asset is not immediately eligible after
being added.

### Size eviction

After an add or during maintenance, the job calculates the sum of raw binary
bytes for present data. If it exceeds `ASSET_CACHE_MAX_SIZE_MB`, it evicts
oldest data first by `last_accessed_at`, then `created_at`, then opaque ID for
determinism. It evicts only enough data to return under the limit and uses
bounded batches.

Access-time updates may be coalesced or write-behind to avoid a database write
on every image request, but the implementation must document the resulting
LRU precision. The eviction query and concurrent writers need row locks,
advisory locking, or an equivalent coordination mechanism so concurrent
evictions cannot exceed the configured policy or delete a blob currently being
served.

### Orphan cleanup

Orphan cleanup is separate from byte eviction. A blob or asset record with no
`article_assets` relationship may have its metadata and data deleted once no
active lease references it. This is the only path that deletes the asset
metadata entry. Age and size eviction delete binary data only.

Maintenance uses PostgreSQL time, survives worker restarts, and is safe to run
on multiple replicas. A short-lived maintenance lease or PostgreSQL advisory
lock prevents duplicate work. Expired fetch/serve/maintenance leases are
recoverable by a later maintenance pass.

## Security and resource limits

Asset URLs originate in upstream HTML and must be treated as an SSRF input.
The downloader must:

- allow only approved HTTP(S) schemes and hosts;
- reject loopback, private, link-local, multicast, and reserved addresses;
- resolve and re-check addresses at connection time to reduce DNS rebinding;
- revalidate every redirect and reject userinfo and unsafe ports;
- enforce connection, total, body, and redirect limits;
- stream and stop before `ASSET_MAX_SIZE_MB` is exceeded;
- validate both declared media type and magic bytes;
- send no account cookies in the first implementation; any future
  authenticated mode must enforce cookie domain/path/secure/expiry matching;
- reject active content such as HTML and unapproved SVG; and
- avoid including cookies, signed URLs, or response bodies in logs/errors.

The asset ID is opaque and unguessable. The route must never become a generic
proxy for an arbitrary URL. If the application later serves assets from the
same origin as the admin UI, the response policy must remain isolated from
HTML and JavaScript content.

## Transaction and recovery rules

Network fetches and hashing happen before the final persistence transaction.
The unit-of-work asset preflight performs raw-byte comparisons at the start of
that transaction, before source/article row locks are acquired, while holding
only ordered checksum advisory locks. The final transaction then atomically
records the blob candidate, URL/version record, article relationship, rewritten
HTML, and any feed revision invalidation without loading large candidate bodies
after those row locks are held.

Blob writes and asset attachment are independently retryable. A worker crash
can leave an unreferenced blob or a missing-data URL record, but cannot leave a
partially attached article relationship. Leases must expire and be recoverable
without deleting referenced data.

PostgreSQL `bytea` storage increases WAL, backup, TOAST, vacuum, and
replication load. Asset download concurrency must be bounded independently of
the general worker count, and the database pool must not be exhausted by large
asset operations.

## Test requirements before implementation is considered complete

Unit tests should keep one behavior and one expected outcome per test. At
minimum they should cover:

- configuration parsing, decimal MB conversion, zero/unlimited behavior, and
  invalid or overflowing values, including the public-pending limit not
  exceeding the global limit;
- URL-key normalization that preserves queries and removes only fragments;
- request-context construction for anonymous `Referer`, `Origin`, and User-Agent;
- anonymous request construction that never emits a `Cookie` header;
- future authenticated-mode cookie domain/path/secure/expiry filtering;
- per-article asset-count, byte, and wall-clock budgets;
- response-size, redirect, media-type, and magic-byte validation;
- checksum match followed by raw-byte equality and raw-byte mismatch;
- stable URL rewriting and preservation of unresolved external URLs.

PostgreSQL integration tests should cover:

- repeated same-URL addition;
- same bytes from different URLs sharing one blob;
- same URL returning changed bytes creating a new version;
- an expired signed URL triggering one bounded public-article refresh and a new
  asset-record version;
- concurrent additions deduplicating without unique-constraint leakage;
- article deletion and source deletion removing only unreferenced metadata;
- age eviction retaining metadata and relationships;
- LRU size eviction under concurrent writers;
- a missing-data request enqueuing one deduplicated repair job without
  performing upstream work;
- concurrent replicas enforcing one cluster-wide pending limit atomically,
  including duplicate-at-cap and public-reservation cases;
- admission classes being assigned by the enqueue origin rather than accepted
  from job payload JSON;
- capacity-full refresh triggers entering `deferred` without consuming the job
  failure budget, then being admitted after capacity is released;
- repair concurrency, public pending, per-asset requeue, and per-asset attempt
  limits;
- a repair worker performing an anonymous fetch with the expected headers;
- a permanent `404`/`410` producing one terminal response and no later repair
  jobs until a new asset observation from normal article acquisition clears the
  state, while re-persisting the same failed article does not;
- status classification that refreshes only provider-confirmed stale signed
  URLs and does not refresh an ordinary `404`;
- a refresh worker discovering a changed signed URL without using account
  cookies and rebuilding the affected feed;
- refresh-lineage cooldown and attempt exhaustion persisting across replacement
  URL versions without creating another refresh job;
- a crashed/expired asset lease being recovered; and
- article persistence succeeding when asset acquisition fails.

The real browser/HTTP integration test may remain ignored or require an
operator-provided WebDriver, but the HTTP fetcher should have deterministic
local-server tests that assert the captured headers and body handling.
