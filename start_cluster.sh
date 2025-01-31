#!/bin/bash

for i in {1..5} # Adjust the range for the number of nodes
do
    PORT=$((9090 + i - 1)) # Port logic: 9000 + node_id - 1
    echo "Starting node $i on port $PORT..."
    gnome-terminal -- bash -c "go run node.go node$i $PORT; exec bash"
done
