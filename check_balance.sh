#!/bin/bash

# 1. Define your sub-account credentials
API_SECRET="krG9K8uNqw2cInkUlTPQnytjj959P1uy6yGfSfm82hdBW9F99IWts6L3ZgCqeIxY"
API_KEY="jvbYLyF1qRaTJGK8NS42A8qadZjmnTF3KXF3gTl0WWzaUF37fnhkDnwlmmAbYYfc"

TIMESTAMP=$(date +%s%3N)

QUERY="timestamp=${TIMESTAMP}&recvWindow=5000"

SIGNATURE=$(echo -n "$QUERY" | openssl dgst -sha256 -hmac "$API_SECRET" | awk '{print $2}')

curl -s \
  -H "X-MBX-APIKEY: $API_KEY" \
  "https://api.binance.com/api/v3/account?${QUERY}&signature=${SIGNATURE}"
