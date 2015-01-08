#!/bin/bash
# Generate SQL commands to create specialized users and set up table ownership in RedShift
# Generated commands should be run by a superuser (e.g. awsuser)
cat << EOF
CREATE USER ai_indexer WITH PASSWORD '$AWS_REDSHIFT_INDEXER_PWD';
CREATE USER ai_frontend WITH PASSWORD '$AWS_REDSHIFT_FRONTEND_PWD';

REVOKE SELECT,INSERT ON TABLE messages FROM ai_frontend;
REVOKE SELECT,INSERT ON TABLE recipients FROM ai_frontend;

EOF