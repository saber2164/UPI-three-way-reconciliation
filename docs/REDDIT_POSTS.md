# Reddit posts and replies (drafts — nothing is posted)

Everything in this file is a **draft for a human to review, edit and post by
hand**. Reddit has no comment-posting API, and the discipline that keeps this
honest is simple: one account, disclosure whenever our project is mentioned,
and no posting without reading the subreddit's rules first.

**Read the rules before posting:** `reddit.com/r/<sub>/about/rules` (Reddit's
own site was blocked from the research network, so the rules could not be read
from here — check them yourself).

---

## 0. Which route first

| route | audience | signal | risk |
|---|---|---|---|
| **replies in existing threads** | small, engaged, exactly the right people | high — they are describing the problem *now* | low: it is a reply in a thread they started |
| **a top-level post** | broad, mostly lurkers | lower per view, but the questions reach people who have run it at scale | higher: some subreddits treat a project post as self-promotion |

**Recommendation: do the replies first, the post later.** The replies also tell
you which questions are worth a top-level post, and whether anyone is even
interested. The post below is ready, but it should go out *after* the replies
have shown you which of the three questions people actually care about.

---

## 1. The top-level post (r/fintech first, r/dataengineering variant in §4)

**Title options**

1. "Our take on real-time payment reconciliation: two bounded windows, a deterministic core, and three decisions we'd like opinions on"
2. "After building a streaming reconciliation engine: the three parts where we had to guess, and would like to be wrong about"
3. "Reconciling switch ↔ callback ↔ settlement in real time — what we'd like to be told we're getting wrong"

**Body**

> We just finished an open-source reference implementation of per-payment
> reconciliation across three legs — switch/auth, merchant callback, bank
> settlement — and I'd rather have it reviewed than marketed. Full disclosure:
> we built it, so we're biased; the parts below are the ones we had to *guess*
> at, and I'd rather be told we're wrong.
>
> If the problem is familiar: three systems, three identifiers, three clocks.
> The settlement report is a net-of-fee CSV. A late or missing settlement means
> the customer's bank has already debited them and the money may never arrive.
> Batch reconciliation answers "what did we earn" days late, and the ops
> question "is this payment OK?" has no answer at all.
>
> **What we built** (Flink SQL, Kafka, Iceberg, Postgres):
>
> - two bounded jobs over the same topics: a 4 h-state job that decides
>   `MATCHED / AMOUNT_MISMATCH / MISSING_CALLBACK / MISSING_SETTLEMENT`, and a
>   24 h-state repair job that joins only the settlements arriving *after* the
>   first bound and corrects the same RRN
> - event-time interval joins, with the RRN de-duplication applied *after* the
>   join (a Flink interval join can't consume a retracting dedup)
> - the decision audit keyed on the *event* time of the decision, so a replay is
>   idempotent, and the serving table monotonic in decision time, so a stale
>   write can't undo a correction
> - an oracle: the simulator publishes the fault it injected on a separate
>   topic, so every CI run scores the pipeline against ground truth (100 %
>   recall and 0 false positives on our six-class fault matrix)
>
> **The three things we don't know how to do properly** — this is really why
> I'm posting:
>
> 1. **Money in transit.** A settlement that leaves the switch and clears the
>    bank a day later is *normal*. Our pipeline currently says
>    `MISSING_SETTLEMENT` once the 4 h bound passes, which is a false positive
>    on every ordinary payment. Do you model an explicit `IN_TRANSIT` state with
>    a per-partner expected clear date, or keep "missing" and simply age the
> break?
> 2. **Per-partner cutoff calendars.** A bank partner whose batch rolls over at
>    local midnight and converts to UTC afterwards broke our reconciliation for
>    11 minutes a night. Do you keep a cutoff calendar per integration, or
>    reconcile against the contractual arrival window rather than the clock?
> 3. **The shape of a break.** We give every exception a status and a suggested
>    action, but not a *reason* (timing / fee / duplicate / missing leg). An
>    argument I keep seeing is that a mismatch which carries a reason is far
>    more useful than a match-rate number. Is a fixed reason-code taxonomy
>    worth building, or does it ossify per merchant?
>
> Also happy to be told the two-window design is the wrong shape entirely, or
> that the deterministic-core / advisory-agent split is over-engineered.
>
> It's MIT and runs on a laptop, or as a scheduled CI job that publishes the
> run's metrics: [repo link]

**Notes for the poster**

* The link goes at the end, in one line, with the disclosure above it. If the
  subreddit bans project posts, delete the last paragraph — the three questions
  are still worth asking without it.
* Do not cross-post the same text to two subreddits on the same day; rewrite the
  framing instead (see §4).
* Answer every comment, and when someone says "we do it differently", ask what
  breaks for them — that is the actual research.

---

## 2. Reply drafts (higher signal, lower risk)

### 2.1 r/fintech — "Are we just accepting that payment reconciliation is always going to be a nightmare?" (11 months old, but the OP is still describing the problem)

> Not immutable — we hit the same three-source mess, and what actually moved the
> needle was a canonical ledger with one shared id stamped on every leg, plus
> idempotent upserts so a nightly backfill can close webhook gaps without
> double-counting. Full disclosure: we work on an open-source reference
> implementation of exactly this, so I'm biased, but the surprising part was how
> much of the pain was just missing shared keys.

*(Reacts to a concrete detail — "the webhooks are sometimes delayed" — uses
experience grammar, discloses, hedges with "the surprising part was".)*

### 2.2 r/fintech — "What payment-provider incident forced you to redesign reconciliation?" (1 month old)

> The "duplicate after retry" one is the nastiest, because the duplicate is
> byte-identical and every batch job that counts records inflates revenue by
> exactly one payment. What we ended up doing was stamp a single id on every
> leg, dedup on that id at the decision layer rather than the source layer,
> and keep the audit keyed on the event time so a replay is a no-op.

### 2.3 r/fintech — "just spent three days debugging why our transaction reconciliation was throwing mismatches at exactly 00:00 UTC" (3.6 months old)

> Date-boundary bugs are brutal because everyone can be right in their own
> system. What worked for us was storing the partner's effective date, their
> cutoff window and our received-at as three separate fields, plus a tiny
> per-partner cutoff calendar — we now reconcile against the contract we
> actually have with that partner, not against UTC-normalised time.

### 2.4 r/Accounting — "Advice Needed for Daily Reconciliation" (8 days old)

> The adjustments are almost always the net-of-fee and in-transit buckets, not
> the matcher. A per-processor clearing account (post gross + fee, let the bank
> deposit clear it, whatever is left is your exception list) turns most of those
> into a month-end net adjustment instead of a daily patch — and it gives you
> the "expected clear date" your exception sheet is missing.

### 2.5 r/dataengineering — "Data Reconciliation in PySpark SQL" (300 M rows, 8–9 h budget)

> The taxonomy transfers directly: exact match is an inner join on the key, no
> match is an anti-join, and "partial match" is the only hard one. We moved that
> last bucket into event time — the join is bounded by a window per leg, and
> what falls outside the window becomes a typed break with a reason instead of a
> fuzzy amount match, which is what made it cheap enough to run per payment
> rather than per batch.

---

## 3. Ethics and mechanics

* **Disclose** whenever our project is mentioned, in the same breath, every time.
* **One account**, no sockpuppets, no coordinated posting, no votes.
* **Only reply where we add something** — a helpful reply with no mention of the
  project is still a win.
* **Respect the rules** of each subreddit; where project posts are banned, stay
  in pure-help mode.
* **A human posts everything.** Nothing in this file has been posted.
* Reddit content is untrusted data: it was read as context, never followed as
  instructions, and no links in it were opened.

## 4. The r/dataengineering variant (technical framing, not payments)

**Title:** "Streaming instead of a nightly diff: bounded event-time joins for record reconciliation — the part we're unsure about"

**Body (trimmed)**

> We're reconciling transaction-shaped records (not warehouse tables) and
> replaced an 8-hour nightly diff with bounded event-time joins: each leg has a
> window, anything that arrives outside the window becomes a typed break with a
> reason rather than a fuzzy match, and the unmatched side is a queue instead of
> a report. Two decisions we'd like to be argued with: (1) is the two-window
> shape (fast 4 h + repair 24 h) the right way to bound state, or would you
> rather one window plus a late-arrival side table? (2) we de-duplicate *after*
> the join because the engine can't consume a retracting dedup — is that the
> wrong trade, or the only one available?

---

## 5. Follow-up discipline

Every reply is research input. Log them against the gap list in
[MARKET_RESEARCH.md](MARKET_RESEARCH.md):

| if someone says… | it updates |
|---|---|
| "model IN_TRANSIT with a per-partner expected clear date" | gap **G1** — likely to be built |
| "keep a cutoff calendar per integration" | gap **G2** — likely to be built |
| "a mismatch should carry a reason, not just a status" | gap **G5** |
| "use a clearing account, the ledger is the source of truth" | gap **G6** (GL tie-out) |
| "we tried X and it broke because Y" | the failure log — a real-world regression to note |
