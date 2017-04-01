#!/usr/bin/env bash

curl -X POST http://claims.local:8080/api/jobs

curl -X POST -d "{type:'Import', options:{sourceDir: '/claim-db/sourceFiles', after: ISODate('2014-01-01'), claimsCollectionFormat: 'claims_new_%s_debug', restartMemoryLimit: -1}}" http://claims.local:8080/api/start

curl -X POST -d "{id:''}}" http://claims.local:8080/api/kill