"""
Avro contracts for the three legs of a UPI-style transaction.

Field names follow what a PSP/merchant actually sees: RRN (12-digit retrieval
reference), payer/payee VPA, UTR on the bank settlement file, and the merchant
callback (webhook).  Amounts are integer paise.

``event_ts`` is a plain Avro ``long`` holding epoch milliseconds rather than an
Avro logical type.  That keeps the producer trivial (no datetime coercion in
fastavro) and makes the Flink DDL explicit:

    event_ts BIGINT,
    event_ts_ltz AS TO_TIMESTAMP_LTZ(event_ts, 3)

Enums are plain strings for the same reason: schema evolution is one less thing
to debug during a demo.
"""
SWITCH = {
    "type": "record", "name": "SwitchTxn", "namespace": "upi",
    "fields": [
        {"name": "rrn", "type": "string"},
        {"name": "txn_id", "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "payer_vpa", "type": "string"},
        {"name": "payee_vpa", "type": "string"},
        {"name": "amount_paise", "type": "long"},
        {"name": "status", "type": "string"},
        {"name": "resp_code", "type": "string"},
        {"name": "event_ts", "type": "long"},
    ],
}

SETTLEMENT = {
    "type": "record", "name": "SettlementRow", "namespace": "upi",
    "fields": [
        {"name": "rrn", "type": "string"},
        {"name": "utr", "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "amount_paise", "type": "long"},
        {"name": "settlement_cycle", "type": "string"},
        {"name": "bank_ref", "type": "string"},
        {"name": "event_ts", "type": "long"},
    ],
}

CALLBACK = {
    "type": "record", "name": "MerchantCallback", "namespace": "upi",
    "fields": [
        {"name": "rrn", "type": "string"},
        {"name": "merchant_id", "type": "string"},
        {"name": "order_id", "type": "string"},
        {"name": "amount_paise", "type": "long"},
        {"name": "ack_status", "type": "string"},
        {"name": "event_ts", "type": "long"},
    ],
}

SCHEMAS = {
    "upi.switch": SWITCH,
    "upi.settlements": SETTLEMENT,
    "upi.callbacks": CALLBACK,
}
