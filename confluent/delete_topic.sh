#!/bin/bash

# Variables
CLUSTER_URL="your_cluster_url"
AUTH_HEADER="Authorization: Basic your_token"
TOPICS=("equipment_failures" "passenger_flow" "ticket_validations" "train_departures" "incidents" "train_arrivals")

# Boucle pour supprimer chaque topic
for TOPIC in "${TOPICS[@]}"
do
  echo "Deleting topic: $TOPIC"
  
  curl -X DELETE \
       -H "$AUTH_HEADER" \
       "$CLUSTER_URL/topics/$TOPIC"
done