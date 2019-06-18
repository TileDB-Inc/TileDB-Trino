#!/bin/bash

if ! ${PRESTO_HOME}/bin/launcher status; then
  ${PRESTO_HOME}/bin/launcher start;
  sleep 2;
fi

printf "Waiting for presto to initialize.."
until ${PRESTO_HOME}/bin/presto-cli-${PRESTO_VERSION}-executable.jar --execute 'SELECT * FROM system.runtime.nodes' &> /dev/null ;
do
  printf ".";
  sleep 1;
  printf ".";
  sleep 1;
done
printf "\n"

${PRESTO_HOME}/bin/presto-cli-${PRESTO_VERSION}-executable.jar --schema tiledb --catalog tiledb "$@"
