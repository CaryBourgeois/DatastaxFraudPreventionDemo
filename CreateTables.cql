ALTER TABLE rtfap.transactions
add date_text text;

ALTER TABLE rtfap.transactions
with default_time_to_live = 86400;

CREATE TABLE rtfap.txn_count_min (
	year int,
	month int,
	day int,
	hour int,
	minute int,
	approved_rate_hr double,
	approved_rate_min double,
	approved_txn_hr int,
	approved_txn_min int,
	ttl_txn_hr int,
	ttl_txn_min int,
	PRIMARY KEY (( year, month, day, hour, minute ))
);
