#!/bin/bash
start=$2
offset=$3
end=$(($start+$offset-1))
location=$1

downloadLocation=$4
block="$location/block."
vocab="$location/vocab."
for i in $(seq $start 1 $end)
do
   b="$block$i"
   v="$vocab$i"
   echo $b
   echo $v
   gsutil cp $b $downloadLocation
   gsutil cp $b $downloadLocation
done