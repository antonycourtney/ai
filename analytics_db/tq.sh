#!/bin/bash
#
# tq -- compile and test a query from inbox_queries.js and send it to sqlview for execution
#
IFS=$(printf '\t')  # get rid of space
echo=on
gulp build_queries
node gensql.js $@ | python ~/home/src/sqlview/svclient.py - 
