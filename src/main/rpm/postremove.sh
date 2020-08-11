#!/bin/bash
rm -rf /usr/local/splendiddata/migration_syncher
if [ ! "$(ls -A /usr/local/splendiddata)" ] 
then
    rmdir /usr/local/splendiddata
fi
