# torcida-news-rag

Projeto para coletar notícias (RSS-first) dos principais clubes de futebol, indexar no Elasticsearch e expor uma API para RAG + um servidor MCP.

## Visão geral
- Airflow orquestra a coleta diária (06:00 America/Fortaleza).
- RSS-first com fallback para feed geral + filtro por keywords.
- Dedup por URL + título normalizado + similaridade (rapidfuzz >= 92).
- Rank por recência + score do portal + keywords + penalidade para opinião/coluna.
- Seleção diária de 3 notícias por clube.
- Indexação em `news_articles` e `news_chunks` (embeddings opcionais).
- API FastAPI prepara briefing (sem chamar LLM).
- MCP server fornece ferramentas para LLM.

## Requisitos
- Docker + Docker Compose

## Como subir
1. Copie o `.env.example` para `.env` e ajuste o que precisar.
   - Se alterar usuário/senha do Postgres, atualize também `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`.
2. Inicialize o Airflow (uma vez):
   ```bash
   docker compose up airflow-init
   ```
3. Suba os serviços:
   ```bash
   docker compose up -d --build
   ```
4. Acesse:
   - Airflow UI: http://localhost:8080
   - API: http://localhost:8000
   - MCP: http://localhost:7010

## Como rodar agora (sem esperar o agendamento)
1. Dispare o DAG manualmente:
   ```bash
   docker compose exec airflow-webserver airflow dags trigger torcida_news_ingestion
   ```
2. Acompanhe no Airflow UI.

## Como habilitar Fortaleza e substituir um clube do Top 10
1. No `.env`, ajuste:
   ```bash
   ENABLE_FORTALEZA=true
   FORTALEZA_REPLACE_CLUB_ID=santos
   ```
2. O pipeline vai desabilitar o clube informado e habilitar Fortaleza.

## Ajustar fontes e clubes
- Edite `services/ingestion/config/sources.yaml` para feeds.
- Edite `services/ingestion/config/clubs.yaml` para keywords e status.

## Bootstrap opcional do Elasticsearch
Se quiser criar os índices manualmente (via host com `curl`):
```bash
ELASTICSEARCH_URL=http://localhost:9200 bash elastic/bootstrap.sh
```

## Quick test com curl
```bash
curl -s http://localhost:8000/health
curl -s http://localhost:8000/clubs
curl -s "http://localhost:8000/news/search?club=flamengo&days=3&size=3"
curl -s "http://localhost:8000/news/daily-picks?club=flamengo"
curl -s -X POST http://localhost:8000/rag/briefing \
  -H 'Content-Type: application/json' \
  -d '{"club":"flamengo","days":3,"topic":"mercado","tone":"neutro","wordpress":true}'
```

## Observações
- RSS-first, sem scraping pesado. O HTML é baixado apenas para extrair snippet e texto curto (limite 6k chars).
- Se um feed específico falhar, usa `FutebolGeral` do mesmo portal + filtro por keywords.
