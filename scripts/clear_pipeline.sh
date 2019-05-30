#!/bin/bash

./clear_redis.py metl.yml

USER=Administrator
read -s -p "Password for $USER: " PASS
curl -X POST "http://Administrator:${PASS}@localhost:8091/pools/default/buckets/tdx_data/controller/doFlush"
curl -X POST "http://Administrator:${PASS}@localhost:8091/pools/default/buckets/tdx_cache/controller/doFlush"


