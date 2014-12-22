#!/bin/bash

i=0
files=()
for file in $(find . -name "*.py" | sort)
do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    files+=" $file"
  fi
  ((i++))
done

CASSANDRA_VERSION=2.1.2 nosetests --total-processes --with-xunit --xunit-file=$CIRCLE_TEST_REPORTS/tests.xml ${files[@]}