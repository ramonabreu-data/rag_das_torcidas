import json
import logging
import re
import base64
import unicodedata
import time
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.utils.trigger_rule import TriggerRule


# ===========================================================================
# CREDENCIAIS E CONFIGURAÇÕES GERAIS
# ===========================================================================
WP_SITE = "https://ups1tride.com"
WP_USER = "ddd"
WP_APP_PASS = "snGZfPrA9l6SWGktAi1yRMVp"
WP_AUTH = "Basic ZGRkOnNuR1pmUHJBOWw2U1dHa3RBaTF5Uk1WcA=="

# Mantido para referência explícita do enunciado
_WP_AUTH_CHECK = "Basic " + base64.b64encode(f"{WP_USER}:{WP_APP_PASS}".encode()).decode()

ES_HOST = os.getenv("ES_HOST") or os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_INDEX = "news_articles"
ES_MAX_DOCS = 80

TZ_FORTALEZA = ZoneInfo("America/Fortaleza")
STATE_KEY_PREFIX = "clube_news_state_"

CLUBS = {
    "flamengo": {"name": "FLAMENGO", "page": "/clube/flamengo/"},
    "palmeiras": {"name": "PALMEIRAS", "page": "/clube/palmeiras/"},
    "corinthians": {"name": "CORINTHIANS", "page": "/clube/corinthians/"},
    "vasco": {"name": "VASCO", "page": "/clube/vasco/"},
    "santos": {"name": "SANTOS", "page": "/clube/santos/"},
    "sao-paulo": {"name": "SÃO PAULO", "page": "/clube/sao-paulo/"},
    "cruzeiro": {"name": "CRUZEIRO", "page": "/clube/cruzeiro/"},
    "atletico-mg": {"name": "ATLÉTICO-MG", "page": "/clube/atletico-mg/"},
    "fortaleza": {"name": "FORTALEZA", "page": "/clube/fortaleza/"},
    "bahia": {"name": "BAHIA", "page": "/clube/bahia/"},
    "gremio": {"name": "GRÊMIO-RS", "page": "/clube/gremio/"},
}

CLUB_PRIORITY = [
    "flamengo",
    "palmeiras",
    "corinthians",
    "vasco",
    "santos",
    "sao-paulo",
    "cruzeiro",
    "atletico-mg",
    "fortaleza",
    "bahia",
    "gremio",
]

NAME_TO_ID = {
    "flamengo": "flamengo",
    "palmeiras": "palmeiras",
    "plameiras": "palmeiras",
    "corinthians": "corinthians",
    "vasco": "vasco",
    "santos": "santos",
    "sao paulo": "sao-paulo",
    "sao-paulo": "sao-paulo",
    "cruzeiro": "cruzeiro",
    "atletico-mg": "atletico-mg",
    "atletico mineiro": "atletico-mg",
    "fortaleza": "fortaleza",
    "bahia": "bahia",
    "1bahia": "bahia",
    "gremio": "gremio",
    "gremio-rs": "gremio",
}


# ===========================================================================
# FUNÇÕES AUXILIARES
# ===========================================================================
def normalize_slug(value: str) -> str:
    """Normaliza string para slug: lowercase, sem acentos, sem chars especiais."""
    value = value or ""
    value = unicodedata.normalize("NFD", value)
    value = "".join(c for c in value if unicodedata.category(c) != "Mn")
    value = value.lower().strip()
    value = re.sub(r"[^a-z0-9\- ]+", "", value)
    value = re.sub(r"\s+", "-", value)
    return value.strip("-")


def _build_normalized_name_map() -> dict:
    mapping = {}
    for raw, club_id in NAME_TO_ID.items():
        mapping[raw.lower().strip()] = club_id
        mapping[normalize_slug(raw)] = club_id
    for club_id in CLUBS:
        mapping[club_id] = club_id
        mapping[normalize_slug(club_id)] = club_id
    return mapping


NORMALIZED_NAME_TO_ID = _build_normalized_name_map()


def get_today_fortaleza() -> str:
    """Retorna data atual no fuso America/Fortaleza no formato YYYY-MM-DD."""
    return datetime.now(TZ_FORTALEZA).strftime("%Y-%m-%d")


def now_fortaleza_iso() -> str:
    """Retorna datetime atual no fuso America/Fortaleza em formato ISO."""
    return datetime.now(TZ_FORTALEZA).isoformat()


def wp_headers() -> dict:
    """Headers padrão para chamadas à API WordPress."""
    return {
        "Authorization": WP_AUTH,
        "Content-Type": "application/json",
    }


def openai_headers() -> dict:
    """Headers para chamadas à API OpenAI."""
    openai_api_key = os.getenv("OPENAI_API_KEY") or Variable.get("OPENAI_API_KEY", default_var=None)
    if not openai_api_key:
        raise AirflowException(
            "OPENAI_API_KEY não configurada. Defina a variável de ambiente OPENAI_API_KEY "
            "ou a Airflow Variable OPENAI_API_KEY."
        )
    return {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }


def extract_og_image(html: str) -> str | None:
    """Extrai URL da og:image do HTML."""
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, html or "", re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]*>", " ", text or "").strip()


def parse_published_at(value: str) -> datetime:
    if not value:
        return datetime(1970, 1, 1, tzinfo=ZoneInfo("UTC"))
    try:
        txt = str(value).replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt
    except Exception:
        return datetime(1970, 1, 1, tzinfo=ZoneInfo("UTC"))


def map_name_to_club_id(raw_value: str) -> str | None:
    if not raw_value:
        return None
    key1 = str(raw_value).lower().strip()
    key2 = normalize_slug(str(raw_value))
    if key1 in NORMALIZED_NAME_TO_ID:
        return NORMALIZED_NAME_TO_ID[key1]
    if key2 in NORMALIZED_NAME_TO_ID:
        return NORMALIZED_NAME_TO_ID[key2]
    if key2 in CLUBS:
        return key2
    return None


def extract_doc_club_id(doc: dict) -> str | None:
    metadata = doc.get("metadata") or {}
    metadata = metadata if isinstance(metadata, dict) else {}

    candidates = [
        doc.get("club_id"),
        doc.get("club"),
        doc.get("club_name"),
        doc.get("team"),
        doc.get("team_name"),
        metadata.get("club_id"),
        metadata.get("club"),
        metadata.get("club_name"),
        metadata.get("team"),
        metadata.get("team_name"),
    ]

    for key in ("clubs", "teams", "club_ids", "tags"):
        values = doc.get(key)
        if isinstance(values, list):
            candidates.extend(values)

    for item in candidates:
        if isinstance(item, dict):
            nested = [
                item.get("id"),
                item.get("slug"),
                item.get("name"),
                item.get("club_id"),
                item.get("club"),
            ]
            for nested_item in nested:
                club_id = map_name_to_club_id(nested_item)
                if club_id:
                    return club_id

        club_id = map_name_to_club_id(item)
        if club_id:
            return club_id
    return None


def extract_doc_url(doc: dict) -> str:
    return (
        doc.get("url")
        or doc.get("link")
        or doc.get("source_url")
        or doc.get("article_url")
        or ""
    )


def compact_doc_for_prompt(doc: dict) -> dict:
    source = doc.get("source_name") or doc.get("source") or ""
    if isinstance(source, dict):
        source = source.get("name") or ""
    return {
        "title": (doc.get("title") or doc.get("headline") or "").strip(),
        "summary": (doc.get("summary") or doc.get("description") or "").strip(),
        "url": extract_doc_url(doc),
        "published_at": (doc.get("published_at") or "").strip(),
        "source_name": str(source).strip(),
    }


def detect_topic_tags(text: str) -> list[dict]:
    """Detecta tags temáticas no texto do artigo."""
    normalized = normalize_slug(text or "")
    catalog = {
        "contratacao": ["contratacao", "contrata", "reforco", "reforcos"],
        "treinador": ["treinador", "tecnico", "comissao tecnica"],
        "demissao": ["demissao", "demitido", "desligado"],
        "lesao": ["lesao", "lesionado", "departamento medico"],
        "libertadores": ["libertadores"],
        "sul-americana": ["sul-americana", "sul americana", "sudamericana"],
        "copa-do-brasil": ["copa-do-brasil", "copa do brasil"],
        "brasileirao": ["brasileirao", "serie a"],
        "classico": ["classico", "derbi", "rival"],
        "renovacao": ["renovacao", "renovou", "renovado"],
        "transferencia": ["transferencia", "mercado", "janela"],
        "financas": ["financas", "divida", "orcamento", "receita"],
        "base": ["base", "sub-20", "sub-17", "categoria de base"],
        "estadio": ["estadio", "arena", "mandante", "gramado"],
        "futebol": ["futebol", "partida", "jogo"],
        "noticias": ["noticia", "noticias", "atualizacao"],
        "futebol-brasileiro": ["futebol-brasileiro", "futebol brasileiro"],
    }

    found = []
    for slug, aliases in catalog.items():
        for alias in aliases:
            alias_norm = normalize_slug(alias)
            if alias_norm and alias_norm in normalized:
                name = slug.replace("-", " ").title()
                found.append({"slug": slug, "name": name})
                break
    return found


def resolve_or_create_wp_term(endpoint: str, slug: str, name: str) -> int | None:
    """Busca um term (category ou tag) no WP pelo slug. Cria se não existir. Retorna o ID."""
    base = f"{WP_SITE}/wp-json/wp/v2/{endpoint}"

    try:
        response = requests.get(
            base,
            headers=wp_headers(),
            params={"per_page": 100, "slug": slug, "_fields": "id,slug,name"},
            timeout=20,
        )
        if response.ok:
            terms = response.json() or []
            if terms:
                return terms[0].get("id")
    except Exception as exc:
        logging.error("Falha ao buscar term endpoint=%s slug=%s erro=%s", endpoint, slug, exc)

    try:
        create_response = requests.post(
            base,
            headers=wp_headers(),
            json={"name": name, "slug": slug},
            timeout=20,
        )
        if create_response.status_code in (200, 201):
            return create_response.json().get("id")

        logging.error(
            "Falha ao criar term endpoint=%s slug=%s status=%s body=%s",
            endpoint,
            slug,
            create_response.status_code,
            create_response.text[:400],
        )
    except Exception as exc:
        logging.error("Erro ao criar term endpoint=%s slug=%s erro=%s", endpoint, slug, exc)

    return None


def build_state(today: str) -> dict:
    return {
        "date": today,
        "posted": {},
        "last_picked_clubs": [],
        "last_run_at": None,
    }


def club_priority_index(club_id: str) -> tuple:
    if club_id in CLUB_PRIORITY:
        return (0, CLUB_PRIORITY.index(club_id))
    return (1, club_id)


# ===========================================================================
# TASKS
# ===========================================================================
def task_load_state(**kwargs):
    ti = kwargs["ti"]
    run_id = kwargs.get("run_id") or "manual_run"
    today = get_today_fortaleza()
    state_key = f"{STATE_KEY_PREFIX}{today}"

    state = build_state(today)
    raw = Variable.get(state_key, default_var=None)

    if raw:
        try:
            loaded = json.loads(raw)
            if loaded.get("date") == today:
                state = loaded
            else:
                logging.info(
                    "State com data antiga encontrado (state.date=%s, hoje=%s). Reiniciando.",
                    loaded.get("date"),
                    today,
                )
        except Exception as exc:
            logging.warning("Falha ao parsear state existente. Reiniciando. erro=%s", exc)

    # Garantia de formato mínimo
    state.setdefault("date", today)
    state.setdefault("posted", {})
    state.setdefault("last_picked_clubs", [])
    state.setdefault("last_run_at", None)

    ti.xcom_push(key="state", value=state)
    ti.xcom_push(key="today", value=today)
    ti.xcom_push(key="run_id", value=run_id)
    ti.xcom_push(key="state_key", value=state_key)

    logging.info("State carregado para %s com %s clubes já postados.", today, len(state["posted"]))
    return state


def task_fetch_elasticsearch(**kwargs):
    ti = kwargs["ti"]
    url = f"{ES_HOST.rstrip('/')}/{ES_INDEX}/_search"
    payload = {
        "size": ES_MAX_DOCS,
        "sort": [{"published_at": "desc"}],
        "query": {"match_all": {}},
    }

    docs = []
    try:
        response = requests.post(url, json=payload, timeout=20)
        if not response.ok:
            logging.warning(
                "Consulta ao Elasticsearch falhou. url=%s status=%s body=%s",
                url,
                response.status_code,
                (response.text or "")[:400],
            )
        response.raise_for_status()
        data = response.json() or {}
        hits = (((data.get("hits") or {}).get("hits")) or [])
        docs = [(item.get("_source") or {}) for item in hits if item.get("_source")]
        logging.info("Elasticsearch retornou %s documentos. host=%s index=%s", len(docs), ES_HOST, ES_INDEX)
    except Exception as exc:
        logging.warning("Falha ao conectar/consultar Elasticsearch: %s", exc)
        docs = []

    ti.xcom_push(key="es_docs", value=docs)
    return docs


def task_fetch_wp_recent_posts(**kwargs):
    ti = kwargs["ti"]

    posts_out = []
    recent_club_ids = []

    url = f"{WP_SITE}/wp-json/wp/v2/posts"
    params = {
        "per_page": 10,
        "_embed": 1,
        "_fields": "id,slug,title,_embedded",
    }

    try:
        response = requests.get(url, headers={"Authorization": WP_AUTH}, params=params, timeout=20)
        response.raise_for_status()
        posts = response.json() or []

        for post in posts:
            title_rendered = ((post.get("title") or {}).get("rendered") or "").strip()
            clean_title = strip_html(title_rendered)
            posts_out.append(
                {
                    "id": post.get("id"),
                    "slug": post.get("slug"),
                    "title": clean_title,
                }
            )

            embedded = post.get("_embedded") or {}
            terms_groups = embedded.get("wp:term") or []
            found_club = None
            for group in terms_groups:
                if not isinstance(group, list):
                    continue
                for term in group:
                    term_slug = term.get("slug") or ""
                    club_id = map_name_to_club_id(term_slug)
                    if club_id:
                        found_club = club_id
                        break
                if found_club:
                    break

            if found_club and found_club not in recent_club_ids:
                recent_club_ids.append(found_club)

        logging.info(
            "WordPress retornou %s posts recentes; clubes recentes detectados: %s",
            len(posts_out),
            recent_club_ids,
        )
    except Exception as exc:
        logging.warning("Falha ao buscar posts recentes no WordPress: %s", exc)

    ti.xcom_push(key="recent_posts", value=posts_out)
    ti.xcom_push(key="recent_club_ids", value=recent_club_ids)
    return {"recent_posts": posts_out, "recent_club_ids": recent_club_ids}


def task_select_clubs(**kwargs):
    ti = kwargs["ti"]
    state = ti.xcom_pull(task_ids="load_state", key="state") or build_state(get_today_fortaleza())
    docs = ti.xcom_pull(task_ids="fetch_elasticsearch", key="es_docs") or []

    grouped = {}
    docs_without_club = 0
    unmapped_examples = []
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        club_id = extract_doc_club_id(doc)
        if not club_id:
            docs_without_club += 1
            if len(unmapped_examples) < 3:
                unmapped_examples.append(
                    {
                        "title": doc.get("title"),
                        "url": extract_doc_url(doc),
                        "club_id": doc.get("club_id"),
                        "club": doc.get("club"),
                        "club_name": doc.get("club_name"),
                        "metadata_keys": sorted(list((doc.get("metadata") or {}).keys()))
                        if isinstance(doc.get("metadata"), dict)
                        else [],
                    }
                )
            continue
        grouped.setdefault(club_id, []).append(doc)

    for club_id in grouped:
        grouped[club_id] = sorted(
            grouped[club_id],
            key=lambda item: parse_published_at(item.get("published_at")),
            reverse=True,
        )

    if len(grouped) < 3:
        logging.warning(
            "Documentos insuficientes por clube. docs_total=%s docs_sem_mapeamento=%s clubes_mapeados=%s exemplos_sem_mapeamento=%s",
            len(docs),
            docs_without_club,
            sorted(grouped.keys()),
            unmapped_examples,
        )
        raise AirflowException(
            f"Não há clubes suficientes com documentos no ES. Encontrados={len(grouped)}, necessário=3"
        )

    posted_today = set((state.get("posted") or {}).keys())
    last_picked = state.get("last_picked_clubs") or []

    all_clubs_with_docs = list(grouped.keys())
    not_posted = [club for club in all_clubs_with_docs if club not in posted_today]
    already_posted = [club for club in all_clubs_with_docs if club in posted_today]

    ordered_clubs = sorted(not_posted, key=club_priority_index) + sorted(already_posted, key=club_priority_index)

    selected = []

    # Regra principal: evitar repetir os 3 clubes do último disparo.
    for club_id in ordered_clubs:
        if club_id in last_picked:
            continue
        selected.append(club_id)
        if len(selected) == 3:
            break

    # Fallback: completa com qualquer clube disponível.
    if len(selected) < 3:
        for club_id in ordered_clubs:
            if club_id in selected:
                continue
            selected.append(club_id)
            if len(selected) == 3:
                break

    if len(selected) < 3:
        raise AirflowException(
            "Não foi possível selecionar 3 clubes mesmo após fallback. "
            f"Disponíveis={ordered_clubs}, já selecionados={selected}"
        )

    selected_payload = []
    for club_id in selected:
        club_data = CLUBS.get(club_id)
        if not club_data:
            continue
        selected_payload.append(
            {
                "club_id": club_id,
                "club_name": club_data["name"],
                "club_page_url": f"{WP_SITE.rstrip('/')}{club_data['page']}",
                "docs_sorted": grouped.get(club_id, []),
            }
        )

    if len(selected_payload) < 3:
        raise AirflowException("Seleção inválida: menos de 3 clubes com dados completos para processamento.")

    ti.xcom_push(key="selected_clubs", value=selected_payload)
    ti.xcom_push(key="selected_club_ids", value=[item["club_id"] for item in selected_payload])

    logging.info("Clubes selecionados neste disparo: %s", [item["club_id"] for item in selected_payload])
    return selected_payload


def task_process_club(index: int, **kwargs):
    ti = kwargs["ti"]

    selected_clubs = ti.xcom_pull(task_ids="select_clubs", key="selected_clubs") or []
    state = ti.xcom_pull(task_ids="load_state", key="state") or {}
    recent_posts = ti.xcom_pull(task_ids="fetch_wp_recent_posts", key="recent_posts") or []
    recent_club_ids = ti.xcom_pull(task_ids="fetch_wp_recent_posts", key="recent_club_ids") or []

    if index >= len(selected_clubs):
        result = {
            "club_id": None,
            "status": "error",
            "wp_post_id": None,
            "error_msg": f"Indice {index} fora da lista de clubes selecionados",
        }
        logging.error("process_club_%s sem clube correspondente na seleção", index)
        ti.xcom_push(key="result", value=result)
        return result

    club = selected_clubs[index]
    club_id = club.get("club_id")
    club_name = club.get("club_name")
    club_page_url = club.get("club_page_url")
    docs_sorted = club.get("docs_sorted") or []

    def fail(stage: str, error_msg: str) -> dict:
        message = f"{stage}: {error_msg}"
        logging.error("club_id=%s etapa=%s mensagem=%s", club_id, stage, error_msg)
        return {
            "club_id": club_id,
            "status": "error",
            "wp_post_id": None,
            "error_msg": message,
        }

    # ETAPA A — Selecionar documento principal
    try:
        if not docs_sorted:
            result = fail("ETAPA_A", "Nenhum documento disponível para o clube")
            ti.xcom_push(key="result", value=result)
            return result
        main_doc = docs_sorted[0]
        related_docs = docs_sorted[1:6]
        main_compact = compact_doc_for_prompt(main_doc)
        related_compact = [
            {
                "title": compact_doc_for_prompt(doc).get("title", ""),
                "summary": compact_doc_for_prompt(doc).get("summary", ""),
            }
            for doc in related_docs
        ]
    except Exception as exc:
        result = fail("ETAPA_A", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA B — Extrair og:image do artigo fonte
    fallback_image_url = None
    try:
        source_url = main_compact.get("url")
        if source_url:
            response = requests.get(source_url, timeout=10)
            if response.ok:
                fallback_image_url = extract_og_image(response.text)
        logging.info("club_id=%s fallback_image_url=%s", club_id, bool(fallback_image_url))
    except Exception as exc:
        logging.warning("club_id=%s ETAPA_B falhou: %s", club_id, exc)

    # ETAPA C — Gerar artigo com GPT-4.1-mini
    title = ""
    slug = ""
    content = ""
    try:
        clubs_this_run = [item.get("club_id") for item in selected_clubs]
        clubs_previous_run = state.get("last_picked_clubs") or []

        user_prompt_payload = {
            "instrucao": "Retorne APENAS JSON válido no formato solicitado.",
            "regras": {
                "palavras": "Obrigatório entre 750 e 1200 palavras.",
                "foco": "90% na notícia principal e 10% de contexto com relacionadas.",
                "factual": "Não inventar dados; usar apenas os dados fornecidos.",
                "links": "Não usar links externos no corpo; incluir link interno para club_page_url.",
                "imagem": "Inserir marcador literal <!--IMAGE_HERE--> no ponto da imagem.",
                "escopo": "Não misturar fatos de outros clubes.",
            },
            "formato_esperado": {
                "status": "ok",
                "posts": [
                    {
                        "club_id": "",
                        "club_name": "",
                        "club_page_url": "",
                        "TITLE": "",
                        "SLUG": "",
                        "CONTENT": "",
                    }
                ],
            },
            "retorno_sem_dados": {"status": "sem_dados", "mensagem": "..."},
            "dados": {
                "club_id": club_id,
                "club_name": club_name,
                "club_page_url": club_page_url,
                "main_doc": main_compact,
                "related_docs": related_compact,
                "contexto_rotacao": {
                    "clubes_deste_disparo": clubs_this_run,
                    "clubes_disparo_anterior": clubs_previous_run,
                    "wp_posts_recentes": recent_posts,
                    "wp_recent_club_ids": recent_club_ids,
                },
            },
        }

        completion_payload = {
            "model": "gpt-4.1-mini",
            "response_format": {"type": "json_object"},
            "max_tokens": 2500,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Você é um jornalista esportivo especializado em futebol brasileiro. "
                        "Escreva artigos factuais, humanos e objetivos com entre 750 e 1200 palavras. "
                        "Retorne APENAS JSON válido no formato solicitado. "
                        "O campo CONTENT deve ser HTML completo, sem markdown."
                    ),
                },
                {
                    "role": "user",
                    "content": json.dumps(user_prompt_payload, ensure_ascii=False),
                },
            ],
        }

        completion_response = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers=openai_headers(),
            json=completion_payload,
            timeout=90,
        )
        completion_response.raise_for_status()

        completion_data = completion_response.json() or {}
        message_content = (
            (((completion_data.get("choices") or [{}])[0]).get("message") or {}).get("content") or ""
        ).strip()

        if not message_content:
            result = fail("ETAPA_C", "Resposta vazia do modelo")
            ti.xcom_push(key="result", value=result)
            return result

        # Tenta parse direto; se vier em bloco ```json```, remove cercas.
        cleaned = re.sub(r"^```(?:json)?\\s*|\\s*```$", "", message_content, flags=re.IGNORECASE | re.DOTALL).strip()

        model_json = json.loads(cleaned)
        if model_json.get("status") == "sem_dados":
            result = fail("ETAPA_C", model_json.get("mensagem", "Modelo retornou sem dados"))
            ti.xcom_push(key="result", value=result)
            return result

        posts = model_json.get("posts") or []
        if not posts:
            result = fail("ETAPA_C", "JSON sem campo posts")
            ti.xcom_push(key="result", value=result)
            return result

        generated = posts[0]
        title = (generated.get("TITLE") or "").strip()
        slug = normalize_slug((generated.get("SLUG") or "").strip())
        content = (generated.get("CONTENT") or "").strip()

        if not title or not slug or not content:
            result = fail("ETAPA_C", "Campos obrigatórios TITLE/SLUG/CONTENT ausentes")
            ti.xcom_push(key="result", value=result)
            return result

        plain_words = re.findall(r"\b\w+\b", strip_html(content), flags=re.UNICODE)
        if not (750 <= len(plain_words) <= 1200):
            logging.warning(
                "club_id=%s ETAPA_C artigo fora da faixa desejada (palavras=%s)",
                club_id,
                len(plain_words),
            )
    except Exception as exc:
        result = fail("ETAPA_C", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA D — Gerar imagem com DALL-E 3
    image_url = None
    try:
        main_summary = main_compact.get("summary") or ""
        image_prompt = (
            "Fotojornalismo esportivo ultrarrealista, futebol brasileiro profissional. "
            f"Clube em foco: {club_name}. "
            f"Contexto: {title}. {main_summary}. "
            "Sem texto, sem logotipos, sem marcas d'água, iluminação natural, "
            "câmera profissional 35mm, estilo editorial esportivo."
        )
        image_prompt = image_prompt[:1000]

        image_response = requests.post(
            "https://api.openai.com/v1/images/generations",
            headers=openai_headers(),
            json={
                "model": "dall-e-3",
                "prompt": image_prompt,
                "size": "1792x1024",
                "quality": "hd",
                "style": "natural",
                "response_format": "url",
                "n": 1,
            },
            timeout=120,
        )
        image_response.raise_for_status()

        image_data = image_response.json() or {}
        image_url = (((image_data.get("data") or [{}])[0]).get("url") or "").strip() or None
    except Exception as exc:
        logging.warning("club_id=%s ETAPA_D DALL-E falhou: %s", club_id, exc)

    if not image_url:
        image_url = fallback_image_url

    if not image_url:
        result = fail("ETAPA_D", "Falha no DALL-E e sem fallback de imagem")
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA E — Download e upload da imagem no WordPress
    media_id = None
    media_url = None
    try:
        image_bytes_response = requests.get(image_url, timeout=30)
        image_bytes_response.raise_for_status()
        image_bytes = image_bytes_response.content

        filename = f"dalle-{club_id}-{int(time.time())}.png"
        media_headers = {
            "Authorization": WP_AUTH,
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "image/png",
        }

        upload_response = requests.post(
            f"{WP_SITE}/wp-json/wp/v2/media",
            headers=media_headers,
            data=image_bytes,
            timeout=60,
        )
        upload_response.raise_for_status()

        media_json = upload_response.json() or {}
        media_id = media_json.get("id")
        media_url = media_json.get("source_url") or image_url

        if not media_id:
            result = fail("ETAPA_E", "Resposta de upload sem media_id")
            ti.xcom_push(key="result", value=result)
            return result
    except Exception as exc:
        result = fail("ETAPA_E", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA F — Inserir imagem no CONTENT
    try:
        figure_html = (
            '<figure class="article-image">\n'
            f'  <img src="{media_url}" alt="{title}"/>\n'
            '  <figcaption>Imagem: <a href="https://openai.com" rel="nofollow noopener">'
            "OpenAI DALL-E 3</a></figcaption>\n"
            "</figure>"
        )

        if "<!--IMAGE_HERE-->" in content:
            content = content.replace("<!--IMAGE_HERE-->", figure_html, 1)
        elif re.search(r"</h1>", content, flags=re.IGNORECASE):
            content = re.sub(r"</h1>", f"</h1>\n{figure_html}", content, count=1, flags=re.IGNORECASE)
        else:
            content = f"{figure_html}\n{content}"
    except Exception as exc:
        result = fail("ETAPA_F", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA G — Resolver categoria do clube no WordPress
    try:
        category_id = resolve_or_create_wp_term("categories", club_id, CLUBS[club_id]["name"])
        if not category_id:
            result = fail("ETAPA_G", "Não foi possível resolver/criar categoria")
            ti.xcom_push(key="result", value=result)
            return result
    except Exception as exc:
        result = fail("ETAPA_G", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA H — Resolver tags do post
    try:
        thematic = detect_topic_tags(f"{title}\n{strip_html(content)}")

        # Tag do clube é obrigatória.
        tag_candidates = [{"slug": club_id, "name": CLUBS[club_id]["name"]}]

        for tag in thematic:
            if tag["slug"] not in [t["slug"] for t in tag_candidates]:
                tag_candidates.append(tag)

        fallback_tags = [
            {"slug": "futebol", "name": "Futebol"},
            {"slug": "noticias", "name": "Noticias"},
            {"slug": "futebol-brasileiro", "name": "Futebol Brasileiro"},
        ]
        for fb in fallback_tags:
            if len(tag_candidates) >= 4:
                break
            if fb["slug"] not in [t["slug"] for t in tag_candidates]:
                tag_candidates.append(fb)

        tag_candidates = tag_candidates[:7]

        existing_map = {}
        slugs_csv = ",".join([tag["slug"] for tag in tag_candidates])
        try:
            existing_resp = requests.get(
                f"{WP_SITE}/wp-json/wp/v2/tags",
                headers=wp_headers(),
                params={"per_page": 100, "slug": slugs_csv, "_fields": "id,slug,name"},
                timeout=20,
            )
            if existing_resp.ok:
                for item in existing_resp.json() or []:
                    existing_map[item.get("slug")] = item.get("id")
        except Exception as exc:
            logging.warning("club_id=%s ETAPA_H falha ao buscar tags existentes: %s", club_id, exc)

        tag_ids = []
        for tag in tag_candidates:
            slug_tag = tag["slug"]
            if slug_tag in existing_map and existing_map[slug_tag]:
                tag_ids.append(existing_map[slug_tag])
                continue

            created_id = resolve_or_create_wp_term("tags", slug_tag, tag["name"])
            if created_id:
                tag_ids.append(created_id)

        # Remove duplicidades mantendo ordem.
        dedup_ids = []
        for tag_id in tag_ids:
            if tag_id not in dedup_ids:
                dedup_ids.append(tag_id)
        tag_ids = dedup_ids

        if len(tag_ids) < 1:
            result = fail("ETAPA_H", "Nenhuma tag resolvida")
            ti.xcom_push(key="result", value=result)
            return result
    except Exception as exc:
        result = fail("ETAPA_H", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA I — Publicar post no WordPress
    try:
        publish_payload = {
            "title": title,
            "content": content,
            "slug": slug,
            "status": "publish",
            "categories": [category_id],
            "tags": tag_ids,
            "featured_media": media_id,
        }

        publish_response = requests.post(
            f"{WP_SITE}/wp-json/wp/v2/posts",
            headers=wp_headers(),
            json=publish_payload,
            timeout=30,
        )
        publish_response.raise_for_status()

        publish_json = publish_response.json() or {}
        if publish_json.get("status") != "publish":
            result = fail("ETAPA_I", f"Post não publicado. status={publish_json.get('status')}")
            ti.xcom_push(key="result", value=result)
            return result

        wp_post_id = publish_json.get("id")
        if not wp_post_id:
            result = fail("ETAPA_I", "Resposta sem id do post")
            ti.xcom_push(key="result", value=result)
            return result

        result = {
            "club_id": club_id,
            "status": "success",
            "wp_post_id": wp_post_id,
            "error_msg": "",
        }
        logging.info("club_id=%s publicado com sucesso. wp_post_id=%s", club_id, wp_post_id)
        ti.xcom_push(key="result", value=result)
        return result
    except Exception as exc:
        result = fail("ETAPA_I", str(exc))
        ti.xcom_push(key="result", value=result)
        return result


def task_save_state(**kwargs):
    ti = kwargs["ti"]

    state = ti.xcom_pull(task_ids="load_state", key="state") or build_state(get_today_fortaleza())
    today = ti.xcom_pull(task_ids="load_state", key="today") or get_today_fortaleza()

    selected = ti.xcom_pull(task_ids="select_clubs", key="selected_clubs") or []
    selected_ids = [item.get("club_id") for item in selected if item.get("club_id")]

    results = [
        ti.xcom_pull(task_ids="process_club_0", key="result") or {},
        ti.xcom_pull(task_ids="process_club_1", key="result") or {},
        ti.xcom_pull(task_ids="process_club_2", key="result") or {},
    ]

    state.setdefault("date", today)
    state.setdefault("posted", {})

    for result in results:
        if result.get("status") == "success" and result.get("club_id"):
            state["posted"][result["club_id"]] = {
                "at": now_fortaleza_iso(),
                "wp_post_id": result.get("wp_post_id"),
            }

    state["last_picked_clubs"] = selected_ids
    state["last_run_at"] = now_fortaleza_iso()

    state_key = f"{STATE_KEY_PREFIX}{today}"
    Variable.set(state_key, json.dumps(state, ensure_ascii=False))

    logging.info("State salvo em %s com posted=%s", state_key, list((state.get("posted") or {}).keys()))
    ti.xcom_push(key="saved_state", value=state)
    return state


def task_log_summary(**kwargs):
    ti = kwargs["ti"]

    results = [
        ti.xcom_pull(task_ids="process_club_0", key="result") or {},
        ti.xcom_pull(task_ids="process_club_1", key="result") or {},
        ti.xcom_pull(task_ids="process_club_2", key="result") or {},
    ]

    success = [item for item in results if item.get("status") == "success"]
    failures = [
        {"club_id": item.get("club_id"), "motivo": item.get("error_msg")}
        for item in results
        if item.get("status") != "success"
    ]

    logging.info("Resumo da execução - total esperado: 3")
    logging.info("Publicados com sucesso: %s", len(success))
    logging.info("Falhas: %s", failures)

    missing_openai_key = any(
        "OPENAI_API_KEY não configurada" in (item.get("motivo") or "") for item in failures
    )
    if missing_openai_key:
        raise AirflowFailException(
            "OPENAI_API_KEY ausente no runtime. Configure em .env ou em Airflow Variable "
            "OPENAI_API_KEY e reexecute a DAG."
        )

    if len(success) == 0:
        raise AirflowException("Nenhum artigo publicado neste disparo")

    return {
        "total": 3,
        "success": len(success),
        "failures": failures,
    }


# ===========================================================================
# DEFINIÇÃO DA DAG
# ===========================================================================
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="clube_news_publisher",
    schedule_interval="0 6-22/2 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=TZ_FORTALEZA),
    catchup=False,
    max_active_runs=1,
    tags=["futebol", "wordpress", "openai", "elasticsearch"],
    default_args=default_args,
) as dag:
    load_state = PythonOperator(
        task_id="load_state",
        python_callable=task_load_state,
        provide_context=True,
    )

    fetch_elasticsearch = PythonOperator(
        task_id="fetch_elasticsearch",
        python_callable=task_fetch_elasticsearch,
        provide_context=True,
    )

    fetch_wp_recent_posts = PythonOperator(
        task_id="fetch_wp_recent_posts",
        python_callable=task_fetch_wp_recent_posts,
        provide_context=True,
    )

    select_clubs = PythonOperator(
        task_id="select_clubs",
        python_callable=task_select_clubs,
        provide_context=True,
    )

    process_club_0 = PythonOperator(
        task_id="process_club_0",
        python_callable=task_process_club,
        provide_context=True,
        op_kwargs={"index": 0},
    )

    process_club_1 = PythonOperator(
        task_id="process_club_1",
        python_callable=task_process_club,
        provide_context=True,
        op_kwargs={"index": 1},
    )

    process_club_2 = PythonOperator(
        task_id="process_club_2",
        python_callable=task_process_club,
        provide_context=True,
        op_kwargs={"index": 2},
    )

    save_state = PythonOperator(
        task_id="save_state",
        python_callable=task_save_state,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
        provide_context=True,
    )

    load_state >> [fetch_elasticsearch, fetch_wp_recent_posts]
    [fetch_elasticsearch, fetch_wp_recent_posts] >> select_clubs
    select_clubs >> [process_club_0, process_club_1, process_club_2]
    [process_club_0, process_club_1, process_club_2] >> save_state
    save_state >> log_summary
