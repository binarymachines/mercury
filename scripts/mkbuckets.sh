#!/bin/bash


USER=Administrator
echo "Issuing bucket-create commands..."
read -s -p "Password for $USER: " PASS


curl -X POST -u Administrator:${PASS} \
  -d 'name=tdx_data' -d 'ramQuotaMB=10000' -d 'authType=none' \
  -d 'replicaNumber=1' \
  -d 'proxyPort=11215' \   
  http://localhost:8091/pools/default/buckets



curl -X POST -u Administrator:${PASS} \
  -d 'name=tdx_journal' -d 'ramQuotaMB=4000' -d 'authType=none' \
  -d 'replicaNumber=1' \
  -d 'proxyPort=11215' \   
  http://localhost:8091/pools/default/buckets

  
