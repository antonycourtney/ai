-- log of download failures by user ID and message ID to enable debugging

DROP table download_failures;

CREATE TABLE download_failures (
  user_id integer,  -- link back to the users table
  message_id varchar(128),  -- message id that experienced the failure
  exception_desc varchar(256), -- first 256 chars of exception message
  log_time timestamp with time zone
);

ALTER TABLE download_failures
  OWNER TO glenmistro;