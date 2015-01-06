-- Table: users

DROP TABLE users;

CREATE TABLE users
(
  id serial NOT NULL PRIMARY KEY,
  real_name varchar(256)
);

ALTER TABLE users
  OWNER TO glenmistro;


DROP TABLE identities;

CREATE TABLE identities
(
  id serial NOT NULL PRIMARY KEY,

  user_id integer, -- Link back to the users table

  provider varchar(128),
  uid varchar(128),

  email varchar(256),

  refresh_token varchar(256),
  access_token varchar(256),
  expires_at timestamp with time zone
);
ALTER TABLE identities
  OWNER TO glenmistro;

DROP TABLE gmail_syncs;

CREATE TABLE gmail_syncs
(
  id serial NOT NULL PRIMARY KEY,

  user_id integer, -- Link back to the users table
  total_messages integer,
  messages_indexed integer,
  status_message varchar(128),
  last_indexed timestamp with time zone,
  last_requested timestamp with time zone,
  last_msg_uid integer
);
ALTER TABLE gmail_syncs 
  OWNER TO glenmistro;
