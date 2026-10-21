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

## 3. What the research says this project is missing

Ranked by how much they would change the thing.

| # | gap | the evidence | what to build |
|---|---|---|---|
| **G1** | **No "money in transit" state.** Real money leaves the switch and clears the bank one or two days later — that is normal, and this project would call it `MISSING_SETTLEMENT` (a false positive). | "the net-of-fee and in-transit deposits are likely behind most of those adjustments"; "bank feeds lag behind settlement dates so timing differences break the matching rules every single time" | an `IN_TRANSIT` state between switch and settlement, with the *expected clear date* per partner; `MISSING_SETTLEMENT` only after that date passes |
| **G2** | **No per-partner cutoff calendar.** The nastiest bug in the research is a partner whose batch rolls over at local midnight and converts to UTC afterwards. | "for about 11 minutes every night our internal ledger and their ledger were technically in different 'days'"; "we built a whole microservice just to manage per-partner cutoff windows... they call it 'the time machine'"; "nobody knows why the settlement cron runs at 23:52 instead of midnight" | store the partner's effective date, its processing/cutoff window and our received-at separately; a cutoff calendar per integration; never let one partner's day boundary leak into the join |
| **G3** | **No fee decomposition.** Fee leakage (MDR) is not the same failure as a wrong amount, and finance treats them differently. | "Fee / MDR leakage vs what we thought we contracted"; "post gross plus a fee line, so the net then matches the bank" | a `fee` leg (gross, fee, net) with a `FEE_LEAKAGE` reason distinct from `AMOUNT_MISMATCH`, compared against the contracted rate |
| **G4** | **No break ownership or escalation.** An exception sheet needs a name against it. | "exceptions sheet per day with entity, amount, type, owner and expected clear date. Anything older than X days gets escalated" | `owner`, `expected_clear_at`, `severity` on the exception queue; an escalation rule |
| **G5** | **Reason ≠ status.** The status says what happened; the reason says *why the leg is not closed*. | "a mismatch isn't just 'unreconciled'; it has a reason: timing, fee, FX, duplicate, or missing leg" | a separate `reason_code` dimension next to the four statuses |
| **G6** | **No clearing account, no GL tie-out.** Finance reconciles to the ledger; the pipeline stops at the transaction. | "use a clearing account per processor. Post gross and fees there, let the bank deposit clear it, and whatever is left over is your exception list" | a per-processor clearing account and a month-end tie-out |
| **G7** | **The audit records decisions, not evidence.** An auditor wants the records that justified the decision. | "reconciliations aren't comparing GL to GL, they need to find some other evidence, ideally external" | a decision-evidence table: which switch/callback/settlement rows supported which decision |
| **G8** | **The three legs are assumed to share an RRN.** In the real world they do not. | "Stamp a single externalid on every charge and payout. Put it in gateway metadata, transfergroup, and your bank remittance/ACH descriptor so each system echoes the same key" | an id-mapping layer (switch RRN ↔ order id ↔ UTR), with the mapping itself audited |
| **G9** | **No FX.** | "timing, fee, FX, duplicate, or missing leg" | out of scope for UPI (INR only) but needed for a cross-border PSP |
| **G10** | **The break queue has no "why".** | "surface exceptions only" | the `suggested_action` exists; a per-reason playbook would complete it |

**G1 and G2 are the ones that would embarrass a production deployment.** G1 makes
the pipeline cry wolf on every normal settlement; G2 is the class of bug that
costs a team three days.

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
* Nothing was posted. Reddit content is treated as untrusted data, never as
  instructions.
