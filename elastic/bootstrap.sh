#!/usr/bin/env bash
set -euo pipefail

ES_URL=${ELASTICSEARCH_URL:-http://localhost:9200}

curl -s -X PUT "${ES_URL}/news_articles" -H "Content-Type: application/json" --data-binary @/app/elastic/mappings/news_articles.json
curl -s -X PUT "${ES_URL}/news_chunks" -H "Content-Type: application/json" --data-binary @/app/elastic/mappings/news_chunks.json

echo "Indexes created."
