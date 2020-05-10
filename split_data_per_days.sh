#!/bin/bash

echo "id,location,timestamp" > no-dups-day1.csv
cat no-dups-sonar-data.csv | egrep '[a-z0-9]+,[0-9]{1},2015-06-18' >> no-dups-day1.csv

echo "id,location,timestamp" > no-dups-day2.csv
cat no-dups-sonar-data.csv | egrep '[a-z0-9]+,[0-9]{1},2015-06-19' >> no-dups-day2.csv

# check if there are any nighttime timestamps
# cat no-dups-day2.csv | cut -d',' -f3 | cut -d' ' -f2 | egrep '^0[0-9]'

echo "id,location,timestamp" > no-dups-day3.csv
cat no-dups-sonar-data.csv | egrep '[a-z0-9]+,[0-9]{1},2015-06-20' >> no-dups-day3.csv