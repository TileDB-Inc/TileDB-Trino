#!/bin/bash

if ! ${PRESTO_HOME}/bin/launcher status; then
  ${PRESTO_HOME}/bin/launcher start;
  sleep 2;
fi

${PRESTO_HOME}/bin/presto-cli-${PRESTO_VERSION}-executable.jar