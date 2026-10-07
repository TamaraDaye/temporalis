#!/usr/bin/env bash


requests=$1
port=$2


for ((i = 1; i <= requests; i++)); do 
    curl -s -X POST "http://localhost:$port/register" \
        -H 'Content-Type: application/json' \
        -d "{\"service\": \"users\", \"port\":$((8000 + i)) }" &
done

wait 

echo "finished"

