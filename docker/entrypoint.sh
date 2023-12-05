#!/bin/bash

if ! ${TRINO_HOME}/bin/launcher status; then
  ${TRINO_HOME}/bin/launcher start;
  sleep 2;
fi

printf "Waiting for trino to initialize.."
until ${TRINO_HOME}/bin/trino-cli-${TRINO_VERSION}-executable.jar --execute 'SELECT * FROM system.runtime.nodes' &> /dev/null ;
do
  printf ".";
  sleep 1;
  printf ".";
  sleep 1;
done
printf "\n"

${TRINO_HOME}/bin/trino-cli-${TRINO_VERSION}-executable.jar --schema tiledb --catalog tiledb "$@"
