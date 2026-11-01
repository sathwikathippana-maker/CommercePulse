# Data Dictionary

See the event contract in the README. All amounts are INR. `event_version` is monotonic per `transaction_id`. `transaction_date` is the reporting/business date and can be corrected by a later event. `event_ts` is event occurrence time; it is not necessarily the same as arrival time. `ingested_at` and `source_file` are pipeline metadata added at ingestion.
