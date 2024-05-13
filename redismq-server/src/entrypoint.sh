#!/bin/sh
echo "The application will start in ${JHIPSTER_SLEEP}s..." && sleep ${JHIPSTER_SLEEP}
exec exec java -jar ${JAVA_OPTS} ${JAVA_AGENT} ${PARAMS}
