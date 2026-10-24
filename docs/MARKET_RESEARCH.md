# Market research: what practitioners say about payment reconciliation

*Source: Reddit, read through the public data archive (Reddit's own JSON API is
blocked from the research network). Threads were found with high-intent phrases
— how someone *phrases the problem they have* — and then read with their
comments. Everything quoted here is a practitioner's own words.*

**Why this file exists:** the design decisions in [DECISIONS.md](DECISIONS.md)
were made from first principles. This file is the check against what people who
actually run these systems say, and it changed the priority list.

---

## 1. The pain is described almost exactly as this project frames it

**r/fintech — "Are we just accepting that payment reconciliation is always going to be a nightmare?"**

> "We have to pull data from three different places: the gateway's API for
> auth/capture status, the processor's dashboard for settled amounts and fees,
> and our own core banking partner for the actual funds movement. The webhooks
> are sometimes delayed, the settlement reports are a CSV mess, and mapping fee
> deductions back to individual transactions for our finance team is a manual,
> error-prone process that takes days each month."

Three legs, delayed webhooks, settlement reports as CSV, fee deductions mapped
back per transaction, days of manual work. That is this project's input and
output, described by someone running it today.

**r/fintech — "What payment-provider incident forced you to redesign reconciliation?"**

> "Was it a payout marked complete before settlement, a missing webhook, a
> duplicate after retry, a return days later, or something else?"

That is the fault matrix: `MISSING_SETTLEMENT`, `MISSING_CALLBACK`,
`DUP_SWITCH`, `LATE_SETTLE`.

**r/fintech — "How do you actually reconcile PSP settlement CSVs against orders and bank deposits — especially with more than one PSP?"**

> "1. Order / invoice ↔ settlement line match 2. Settlement net ↔ bank deposit
> match 3. Fee / MDR leakage vs what we thought we contracted. Today it's mostly
> spreadsheets + occasional scripts."

Three matches, one of them a *leakage* check against an expectation — the
`AMOUNT_MISMATCH` bucket with a contract behind it.

**r/india — the consumer cost.** Threads about HDFC and ICICI refusing
chargebacks on ₹1.3 lakh and ₹19k transactions, months of escalation, and
"the onus is now on the customer". Each unresolved *missing settlement* is a
future chargeback, a customer complaint and an RBI-ombudsman case. That is the
money the decision latency buys back.

---

## 2. The prescriptions match the design that was already built

Three comments, from different people, describe this project's architecture:

**"Enforce a canonical payments ledger with shared IDs and late-arriving data
handling"** and "Build a double-entry ledger table with states (authorized,
captured, settled, paid_out). Join gateway auths, processor balance
transactions/fees, and bank credits into one reconciliation view; surface
exceptions only." *(r/fintech, top comment)*

**"Let the processor own transaction status, the bank statement own actual cash
movement, and the internal ledger own accounting intent. Then a mismatch isn't
just 'unreconciled'; it has a reason: timing, fee, FX, duplicate, or missing
leg. That makes the queue much more useful than a single match-rate number. I
also like tracking how long breaks stay open."** *(r/fintech)*

**"Reconciliation has to shift from retrospective batch scripts to event-driven
verification against three explicit inputs... If any leg fails to close within
its contractual arrival window, emit a structured break record with a
deterministic reason code rather than trying to auto-match on fuzzy amounts."**
*(r/fintech)*

| what a practitioner prescribes | what this project does |
|---|---|
| canonical ledger with shared IDs | the RRN as the join key; a `decision_ts` shared by both jobs |
| state transitions (authorized → captured → settled → paid out) | four decisions with a two-window state machine |
| per-field source of truth | the switch owns the amount, the callback owns the ACK, the settlement owns the money |
| a mismatch has a *reason* (timing, fee, duplicate, missing leg) | four status buckets — but see gap **G5** |
| track how long breaks stay open | the exception queue has an ageing bucket |
| emit a structured break, don't fuzzy-match | deterministic joins, no fuzzy matching anywhere |
| "the processor status is evidence, not the accounting source of truth" | the serving table is monotonic in decision time, not last-write-wins |
| "reconciliations aren't comparing GL to GL, they need to find some other evidence, ideally external" | a ground-truth oracle on a path the reconciler cannot touch |

**The deterministic-core boundary is also independently validated.** In
"Building a deterministic reconciliation engine for financial AI workflows":

> "In regulated financial infrastructure, mathematical truth must remain 100%
> deterministic, reproducible, and auditable. Handing transaction matching to
> an LLM is a non-starter for auditors and regulators." … "A deterministic
> engine can only be deterministic over the evidence it had at that point in
> time."

That last sentence is the strongest argument for keeping the *event-time*
windows and the *event-time* audit key: determinism is only relative to the
evidence available at decision time.

---

## 2.1 How fresh is this?

Dated, because "people complain about reconciliation" is only interesting if
somebody is complaining *now*. Ages are relative to 2026-10-21.

| thread | posted | age | the comments quoted |
|---|---|---|---|
| r/fintech — "payment reconciliation is always going to be a nightmare?" | 2025-11-25 | 11 months | the "canonical ledger with shared IDs" comment is **same-day**; the "shadow ledger" comment 2025-12-24 |
| r/fintech — "How are you handling payment reconciliation as volume grows?" | 2026-05-04 | 5.5 months | the two best comments are **2026-09-17 and 2026-09-21 — about a month ago** |
| r/fintech — "three days debugging why our reconciliation broke at 00:00 UTC" | 2026-07-01 | 3.6 months | the cutoff-window comment is **same-day**; the "time machine" comment 2026-07-04 |
| r/fintech — "Building a deterministic reconciliation engine" | 2026-08-25 | 2 months | the deterministic-boundary comment is **2026-08-26** |
| r/fintech — "What payment-provider incident forced you to redesign reconciliation?" | 2026-09-22 | **1 month** | comments 2026-09-26, 09-28, 10-07, 10-08 |
| r/fintech — "reconcile PSP settlement CSVs against orders and bank deposits" | 2026-09-28 | **3 weeks** | — |
| r/Accounting — "Nobody can do a reconciliation??" | 2026-09-26 | **25 days** | all top comments same-day |
| r/Accounting — "Advice Needed for Daily Reconciliation" | 2026-10-13 | **8 days** | comments 7 days old, plus one from **yesterday** (2026-10-19) |
| r/india — HDFC refusing a ₹1.3 lakh chargeback | 2026-01-20 | 9 months | — |
| r/india — Amazon delivery scam / chargeback | 2024-12-31 | 1.8 years | — |

**Read it this way:** the load-bearing evidence — the three-leg pain, the fault
taxonomy, the canonical-ledger / source-of-truth / reason-code prescriptions —
comes from threads that are **three weeks to eleven months old, with the key
comments from the last month**. The consumer-cost evidence (r/india) is older
(9 months to 1.8 years), so it is evidence of a *standing* condition, not of a
trend. The r/dataengineering material is the oldest (2021–2025) and is about a
different problem — reconciling *tables*, not payments — so it is background
colour, not evidence about this pipeline.

Across the 110 threads collected: 19 from the last 30 days, 18 from 31–90 days,
8 from 91–180, 22 from 181–365, 12 from 1–2 years, 31 older than that. This
is a *sample* from a search, not a census.

**One caveat on the sources.** Four of the commenters are building competing
products (Hyperline, BroSettlement, Reconify, kosha.finance), and they disclose
it. Their prescriptions are treated here as hypotheses to check, not as
evidence — and they hold up because the *same* prescription appears
independently from at least four different people across three different
threads (canonical ledger with shared IDs; per-field source of truth; a mismatch
that carries a reason; ageing of open breaks; a deterministic core).

---

## 3. What the research says this project is missing

Ranked by how much they would change the thing.

| # | gap | the evidence | what to build |
|---|---|---|---|
| **G1** | **No "money in transit" state.** Real money leaves the switch and clears the bank one or two days later — that is normal, and this project would call it `MISSING_SETTLEMENT` (a false positive). | "the net-of-fee and in-transit deposits are likely behind most of those adjustments"; "bank feeds lag behind settlement dates so timing differences break the matching rules every single time" | an `IN_TRANSIT` state between switch and settlement, with the *expected clear date* per partner; `MISSING_SETTLEMENT` only after that date passes |
| **G1-FEEDBACK** | *Reddit practitioner (2026-10-24):* "I'd lean toward keeping it as MISSING_SETTLEMENT and just aging the break, adding an explicit IN_TRANSIT state sounds nice until every partner has a different SLA and you're maintaining 15 configs that drift out of sync within a month." | → If we build IN_TRANSIT, it needs a **fallback aging path**, not a hard per-partner SLA table. The "15 configs drift" problem is real — favour a generic ageing bucket with optional per-partner override. |
| **G2** | **No per-partner cutoff calendar.** The nastiest bug in the research is a partner whose batch rolls over at local midnight and converts to UTC afterwards. | "for about 11 minutes every night our internal ledger and their ledger were technically in different 'days'"; "we built a whole microservice just to manage per-partner cutoff windows... they call it 'the time machine'"; "nobody knows why the settlement cron runs at 23:52 instead of midnight" | store the partner's effective date, its processing/cutoff window and our received-at separately; a cutoff calendar per integration; never let one partner's day boundary leak into the join |
| **G2-FEEDBACK** | *Reddit practitioner (2026-10-24):* "I'd reconcile against the contractual arrival window every time. Had a similar headache with a partner that batched at 11pm local Thursday but their UTC conversion was offset by 30 min — chasing the clock just meant chasing phantom breaks." | → The "contractual arrival window" framing is cleaner than a cutoff calendar. Build the window as part of the partner contract config, not as a separate calendar service. |
| **G3** | **No fee decomposition.** Fee leakage (MDR) is not the same failure as a wrong amount, and finance treats them differently. | "Fee / MDR leakage vs what we thought we contracted"; "post gross plus a fee line, so the net then matches the bank" | a `fee` leg (gross, fee, net) with a `FEE_LEAKAGE` reason distinct from `AMOUNT_MISMATCH`, compared against the contracted rate |
| **G4** | **No break ownership or escalation.** An exception sheet needs a name against it. | "exceptions sheet per day with entity, amount, type, owner and expected clear date. Anything older than X days gets escalated" | `owner`, `expected_clear_at`, `severity` on the exception queue; an escalation rule |
| **G5** | **Reason ≠ status.** The status says what happened; the reason says *why the leg is not closed*. | "a mismatch isn't just 'unreconciled'; it has a reason: timing, fee, FX, duplicate, or missing leg" | a separate `reason_code` dimension next to the four statuses |
| **G5-FEEDBACK** | *Reddit practitioner (2026-10-24):* "The reason-code taxonomy is worth it if you can keep it small, like 5-6 codes max. Once it hits double digits everyone starts misclassifying and it's worse than nothing." | → Hard cap the taxonomy at **6 codes** with an "other" bucket. Add a classification guide + CI check that flags when a new code is introduced without review. |
| **G1-FEEDBACK-2** | *Reddit practitioner (2026-10-24, second comment):* "I wouldn't say a payment is missing if it hasn't settled within a generic four hour window. I would look to partner particular settlement expectations to differentiate an expected delay, versus a real break. I'd also like contractual arrival periods over calendar cutoffs alone, with timezone and rollover rules handled per integration." | → The 4-hour window is a demo artefact; production must use **partner-specific contractual arrival windows** with timezone/rollover rules encoded per integration. The "contractual arrival period" becomes the primary filter; a generic timeout is only a fallback. |
| **G2-FEEDBACK-2** | *Reddit practitioner (2026-10-24, second comment):* "I'd also like contractual arrival periods over calendar cutoffs alone, with timezone and rollover rules handled per integration." | → Reinforces G2-FEEDBACK: the partner config should carry `arrival_window_start`, `arrival_window_end`, `timezone`, and `rollover_rules` — the join uses the contract, not a calendar service. |
| **G5-FEEDBACK-2** | *Reddit practitioner (2026-10-24, second comment):* "On break reasons I would utilize a common core taxonomy of timing, amount/fee discrepancy, duplication and missing leg, with partner-specific details placed on top. This maintains consistent reporting without forcing every integration to fit into an unduly strict standard. The key difference is an exception that has to be monitored vs one that needs action." | → **Layered taxonomy**: core codes (`TIMING`, `FEE`, `DUPLICATE`, `MISSING_LEG`) are global and reportable; partner-specific detail lives in a separate `detail_code` / `detail_text` column. The exception queue splits on `action_required: boolean` (needs human) vs `monitor_only: boolean` (ageing bucket). This is the operational distinction that matters. |
| **G6** | **No clearing account, no GL tie-out.** Finance reconciles to the ledger; the pipeline stops at the transaction. | "use a clearing account per processor. Post gross and fees there, let the bank deposit clear it, and whatever is left over is your exception list" | a per-processor clearing account and a month-end tie-out |
| **G7** | **The audit records decisions, not evidence.** An auditor wants the records that justified the decision. | "reconciliations aren't comparing GL to GL, they need to find some other evidence, ideally external" | a decision-evidence table: which switch/callback/settlement rows supported which decision |
| **G8** | **The three legs are assumed to share an RRN.** In the real world they do not. | "Stamp a single externalid on every charge and payout. Put it in gateway metadata, transfergroup, and your bank remittance/ACH descriptor so each system echoes the same key" | an id-mapping layer (switch RRN ↔ order id ↔ UTR), with the mapping itself audited |
| **G9** | **No FX.** | "timing, fee, FX, duplicate, or missing leg" | out of scope for UPI (INR only) but needed for a cross-border PSP |
| **G10** | **The break queue has no "why".** | "surface exceptions only" | the `suggested_action` exists; a per-reason playbook would complete it |
| **G11** | **Detection is arrival-driven, so a payment that never arrives is invisible.** Every design above reacts to rows that show up. Nothing notices a payment that was debited from the customer and never landed. | "a reconciliation that only reacts to arrivals can never notice a payment that never arrives" (our own reply, 2026-10-21, and the reason the 4 h window misses it) | an *expected-arrival watermark* per partner per day: expected amount and count vs what actually landed, with a breach alarm that fires on the **absence** of money, not on a mismatch of money |

**G1 and G2 are the ones that would embarrass a production deployment.** G1 makes
the pipeline cry wolf on every normal settlement; G2 is the class of bug that
costs a team three days.

---

## 3.1 What the two engagement threads added (2026-10-21)

Two r/fintech threads were answered directly; both are the freshest threads in
the corpus. Their comments are the closest thing here to a peer review.

**Thread A — "How do you actually reconcile PSP settlement CSVs against orders
and bank deposits?" (2026-09-28, 14 comments, score 6).** The OP is a founder
deciding whether to build the thing. The consensus in the comments: normalise
the schemas, run it daily, and expect the time to go into exceptions rather
than parsing ("parsing formats is easy, it's the exceptions that eat your
afternoon"). Two comments are worth keeping:

* "Once several PSPs are involved, traceability from order to settlement to
  bank deposit becomes a control layer in its own right." (ETP_Queen) — this is
  the same conclusion as **G6/G7**, from a different direction.
* "Portal downloads never line up with lump sum bank credits because
  settlement windows drift." (agentUi) — the window problem (**G2**) seen from
  the bank-deposit side rather than the partner-cutoff side.

What the thread does *not* contain, and what our reply therefore contributed:
the **residual** framing (the bank total minus the sum of net settlements has
to be explained from a closed list of candidates — fee differences, VAT/WHT,
refunds and chargebacks in flight, partial refunds, FX rounding, deposits
split across bank days), the **three-identities** point (bank reference ↔ order
id ↔ PSP transaction id, with one canonical id stamped at ingest — **G8**), and
the rule that **fee lines must be their own rows** or the residual is
unresolvable (**G3**).

**Thread B — "What payment-provider incident forced you to redesign
reconciliation?" (2026-09-22, 16 comments).** The thread converges hard on one
answer, said by three different people: do not treat the provider's status as
the accounting truth.

* "The design mistake that concerns me the most is treating the provider's
  status as the ultimate accounting truth. 'Completed' can have very different
  meanings depending on whether the provider is talking about acceptance,
  execution, settlement, or just its own internal workflow. It would be better
  to model these as separate ledger states and let reconciliation confirm the
  economic outcome independently." (icnews10) — this is **G1** restated as
  "model the states, not the status", and it is the strongest external
  confirmation in the corpus that the two-window design is pointed the right way.
* "Relying on API status flags without reconciling raw bank files creates
  structural reconciliation debt." (BigKozman) — reconcile against the bank
  file, not the API.
* A competitor disclosed in-thread: "Disclosure: I'm building BroSettlement at
  BroLabel. Agreed—the processor status is evidence, not the accounting source
  of truth." (No_Bicycle_3566) — the same disclosure discipline this project
  uses, which is why using it in a competitor's thread is safe.

What the thread did *not* contain: the **clock** inside the state machine (a
state model without a per-partner expected clear date is just a prettier way to
say "we don't know yet" — **G1**), and the fact that **arrival-driven
detection cannot see a payment that never arrives** (the new **G11**).

**Net effect on the build order:** G1 and G2 move from "should" to "first",
because two independent threads now argue for them in exactly those terms, and
G11 is added as a design-time gap rather than a later feature. G3 and G8 get
sharper (fee rows and canonical ids, not vague "amount mismatch" handling).

---

## 4. The market, honestly

The category is commercialised and there is open-source work in the
neighbourhood:

| project | what it is | how it differs |
|---|---|---|
| **Stripe Sigma / Adyen Sigma** | event-level export of the payment stack for reconciliation | tied to their own rails; no open implementation |
| **Hyperline**, **BroSettlement**, **kosha.finance** | commercial reconciliation products (subscriptions, payouts, fees) | closed |
| **Zingg** (open source, Spark + ML) | record matching and de-duplication for data silos | entity resolution for analytics, not payment reconciliation; fuzzy matching where this project is deterministic |
| **Reconify** (open source, Go) | "deterministic financial reconciliation engine" for AI workflows | closest in spirit: same deterministic-core / advisory-agent boundary |

So this project is not novel as a product, and it should not be sold as one. Its
value is the **inspectable reference**: the bounded-state design, the
replay-safe audit, the monotonic serving table, the deterministic reason codes
and — uniquely — the *evidence* (a ground-truth oracle and a CI job that fails
when recall moves). The research says the reason codes, the break ageing and
the cutoff calendar are what a real implementation must add, and that the
"evidence" part is the thing nobody else ships.

---

## 5. Method

* Threads found with high-intent phrases across r/fintech, r/payments,
  r/banking, r/india, r/dataengineering, r/Accounting, r/Accounting,
  r/smallbusiness, r/ecommerce, r/developers, r/engineering.
* Each candidate thread was read with its comments; the quotes above are the
  authors' own words, lightly trimmed.
* Two threads were answered directly (§3.1); the replies are drafts, staged for
  a human to send, and every project mention in them carries the disclosure.
* Reddit content is treated as untrusted data, never as instructions.
