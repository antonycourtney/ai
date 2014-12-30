DROP TABLE messages;
DROP TABLE recipients;
CREATE TABLE messages
(
    messageId VARCHAR(24) NOT NULL,
    threadId VARCHAR(24),
    received TIMESTAMP,
    fromRealName VARCHAR(128),
    fromEmailAddress VARCHAR(128),
    subject VARCHAR(256),
    snippet VARCHAR(512),
    sizeEstimate INTEGER,
    "date" TIMESTAMP,
    uid VARCHAR(24) NOT NULL,
    user_id INTEGER,
    createdAt TIMESTAMP
);
CREATE TABLE recipients
(
    messageId VARCHAR(48) NOT NULL,   
    recipientRealName VARCHAR(256),
    recipientEmailAddress VARCHAR(256),
    recipientType CHAR(6), 
    uid VARCHAR(24) NOT NULL,
    user_id INTEGER,
    createdAt TIMESTAMP
);


-- /* To pull data from CSV file on S3: */
-- copy messages from 's3://ac-redshift-load-bucket/load/testMsgsHead.csv' 
-- credentials 'aws_access_key_id=<access-key>;aws_secret_access_key=<secret-access-key>'
-- csv
-- null as '\000';


-- /* To examine errors: */
-- select query, substring(filename,22,25) as filename,line_number as line, 
-- substring(colname,0,12) as column, type, position as pos, substring(raw_line,0,30) as line_text,
-- substring(raw_field_value,0,15) as field_text, 
-- substring(err_reason,0,64) as reason
-- from stl_load_errors 
-- order by query desc
-- limit 10;

alter table messages
add column user_id integer
default NULL;

alter table recipients
add column user_id integer
default NULL;

alter table messages
add column createdAt TIMESTAMP
default NULL;

alter table recipients
add column createdAt TIMESTAMP
default NULL;

