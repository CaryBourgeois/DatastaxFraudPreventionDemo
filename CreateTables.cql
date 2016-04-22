ALTER TABLE rtfap.transactions
add date_text text;

ALTER TABLE rtfap.transactions
with default_time_to_live = 86400;

CREATE TABLE IF NOT EXISTS rtfap.txn_count_sec (
	epoch_sec bigint,
	ts timestamp,
	total_txn int,
	approved_txn int,
	declined_txn int,
	PRIMARY KEY (epoch_sec, ts)
) WITH CLUSTERING ORDER BY ( ts DESC )

CREATE TABLE IF NOT EXISTS rtfap.txn_count_min (
	epoch_min bigint,
	ts timestamp,
	total_txn int,
	approved_txn int,
	declined_txn int,
	PRIMARY KEY (epoch_sec, ts)
) WITH CLUSTERING ORDER BY ( ts DESC )