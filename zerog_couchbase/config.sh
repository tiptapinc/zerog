#!/bin/sh
./entrypoint.sh couchbase-server &

until $(curl --output /dev/null --silent --head --fail http://localhost:8091); do
    printf '.'
    sleep 1
done

couchbase-cli cluster-init -c localhost \
	--cluster-username Administrator \
	--cluster-password password \
	--services data,index,query \
	--cluster-ramsize 1024 \
	--cluster-index-ramsize 256

couchbase-cli bucket-create -c localhost \
	--username Administrator \
	--password password \
	--bucket test \
	--bucket-type couchbase \
	--bucket-ramsize 1024

wait
