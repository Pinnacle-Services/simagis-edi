#!/usr/bin/env bash

echo /PayPredict/paypredict-R/
mkdir /PayPredict/paypredict-R/
cp image/paypredict-R/* /PayPredict/paypredict-R/

echo /srv/shiny-server/
mkdir /srv/shiny-server/
cp -r image/shiny-server/* /srv/shiny-server/

echo mongorestore
mongorestore --drop --gzip --archive=image/mongo/mongodump.gz

echo restart shiny-server
systemctl restart shiny-server

echo restart tomcat
systemctl restart tomcat
