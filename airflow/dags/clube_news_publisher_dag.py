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
WP_SITE_DEFAULT = "https://ups1tride.com"
WP_USER_AGENT_DEFAULT = "n8n"

ES_HOST = os.getenv("ES_HOST") or os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_INDEX = "news_articles"
ES_MAX_DOCS = 80

TZ_FORTALEZA = ZoneInfo("America/Fortaleza")
STATE_KEY_PREFIX = "clube_news_state_"
USED_DOCS_VARIABLE = "clube_news_used_docs"

CLUBS = {
    "flamengo": {"name": "FLAMENGO", "page_url": "https://ups1tride.com/flamengo/"},
    "palmeiras": {"name": "PALMEIRAS", "page_url": "https://ups1tride.com/plameiras/"},
    "corinthians": {"name": "CORINTHIANS", "page_url": "https://ups1tride.com/corinthians/"},
    "vasco": {"name": "VASCO", "page_url": "https://ups1tride.com/vasco/"},
    "santos": {"name": "SANTOS", "page_url": "https://ups1tride.com/santos/"},
    "sao-paulo": {"name": "SÃO PAULO", "page_url": "https://ups1tride.com/sao-paulo/"},
    "cruzeiro": {"name": "CRUZEIRO", "page_url": "https://ups1tride.com/cruzeiro/"},
    "atletico-mg": {"name": "ATLÉTICO-MG", "page_url": "https://ups1tride.com/atletico-mg/"},
    "fortaleza": {"name": "FORTALEZA", "page_url": "https://ups1tride.com/fortaleza/"},
    "bahia": {"name": "BAHIA", "page_url": "https://ups1tride.com/bahia/"},
    "gremio": {"name": "GRÊMIO-RS", "page_url": "https://ups1tride.com/gremio-rs/"},
}

CLUB_PAGE_SLUGS = {
    "flamengo": "flamengo",
    "palmeiras": "plameiras",
    "corinthians": "corinthians",
    "vasco": "vasco",
    "santos": "santos",
    "sao-paulo": "sao-paulo",
    "cruzeiro": "cruzeiro",
    "atletico-mg": "atletico-mg",
    "fortaleza": "fortaleza",
    "bahia": "bahia",
    "gremio": "gremio-rs",
}

CLUB_PAGE_URLS = {
    "flamengo": "https://ups1tride.com/flamengo/",
    "palmeiras": "https://ups1tride.com/plameiras/",
    "corinthians": "https://ups1tride.com/corinthians/",
    "vasco": "https://ups1tride.com/vasco/",
    "santos": "https://ups1tride.com/santos/",
    "sao-paulo": "https://ups1tride.com/sao-paulo/",
    "cruzeiro": "https://ups1tride.com/cruzeiro/",
    "atletico-mg": "https://ups1tride.com/atletico-mg/",
    "fortaleza": "https://ups1tride.com/fortaleza/",
    "bahia": "https://ups1tride.com/bahia/",
    "gremio": "https://ups1tride.com/gremio-rs/",
}

CLUB_COLORS = {
    "flamengo": "vermelho e preto",
    "palmeiras": "verde e branco",
    "corinthians": "preto e branco",
    "vasco": "preto e branco",
    "santos": "branco e preto",
    "sao-paulo": "vermelho, preto e branco",
    "cruzeiro": "azul e branco",
    "atletico-mg": "preto e branco",
    "fortaleza": "azul, vermelho e preto",
    "bahia": "azul e vermelho",
    "gremio": "azul, preto e branco",
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

DATA_CONTRACT = {
    "instrucoes_criticas": [
        "Escreva APENAS com base nos campos title, summary, snippet e url de main_doc.",
        "Se summary e snippet estiverem vazios, use apenas o title disponível.",
        "Nunca inferir, completar ou extrapolar informações além do que está nos dados.",
        "Nunca mencionar jogadores, técnicos, placares ou datas que não estejam nos dados.",
        "Nunca citar nomes de pessoas que não apareçam literalmente nos dados recebidos.",
        "Nunca inventar declarações ou falas entre aspas.",
        "Se os dados forem insuficientes para 750 palavras, expanda o contexto histórico",
        "do clube usando apenas fatos amplamente conhecidos do futebol brasileiro,",
        "deixando claro que são informações de contexto geral, não da notícia em questão.",
        "Nunca misturar informações de related_docs com o clube principal do artigo.",
        "Usar related_docs apenas como contexto secundário, nunca como fato principal.",
    ],
    "proibicoes_absolutas": [
        "Não inventar transferências, contratações ou rescisões não confirmadas nos dados.",
        "Não inventar resultados de jogos não mencionados nos dados.",
        "Não inventar escalações, lesões ou suspensões não mencionadas nos dados.",
        "Não usar frases como 'segundo fontes', 'soube apurar' ou similares.",
        "Não citar sites, portais ou veículos de comunicação concorrentes.",
        "Não incluir links para domínios externos ao ups1tride.com.",
        "Não repetir o mesmo ângulo ou lide de artigos anteriores listados em recent_posts.",
    ],
}

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


def get_env_or_variable(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is not None and str(value).strip() != "":
        return value
    return Variable.get(name, default_var=default)


def as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def as_int(value: str | None, default: int) -> int:
    try:
        return int(str(value))
    except Exception:
        return default


def wp_base_url() -> str:
    raw_site = get_env_or_variable("WP_SITE", WP_SITE_DEFAULT) or WP_SITE_DEFAULT
    return raw_site.strip().rstrip("/")


def wp_verify_ssl() -> bool:
    allow_unauthorized = as_bool(get_env_or_variable("WP_ALLOW_UNAUTHORIZED_CERTS", "false"), default=False)
    return not allow_unauthorized


def wp_auth_header() -> str:
    explicit_auth = get_env_or_variable("WP_AUTH", None)
    if explicit_auth:
        return explicit_auth

    wp_user = get_env_or_variable("WP_USER", None)
    wp_pass = get_env_or_variable("WP_APP_PASS", None)
    if wp_user and wp_pass:
        encoded = base64.b64encode(f"{wp_user}:{wp_pass}".encode()).decode()
        return f"Basic {encoded}"

    raise AirflowException(
        "Credenciais do WordPress ausentes. Defina WP_USER e WP_APP_PASS "
        "(ou WP_AUTH) via variável de ambiente ou Airflow Variable."
    )


def wp_headers(extra_headers: dict | None = None, include_auth: bool = True) -> dict:
    """Headers padrão estilo n8n para chamadas à API WordPress."""
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": get_env_or_variable("WP_USER_AGENT", WP_USER_AGENT_DEFAULT) or WP_USER_AGENT_DEFAULT,
    }
    if include_auth:
        headers["Authorization"] = wp_auth_header()
    if extra_headers:
        headers.update(extra_headers)
    return headers


def wp_request(
    method: str,
    resource: str,
    *,
    params: dict | None = None,
    json_payload: dict | list | None = None,
    data: bytes | str | None = None,
    headers: dict | None = None,
    timeout: int | None = None,
    include_auth: bool = True,
    retries: int | None = None,
) -> requests.Response:
    """Cliente WP centralizado (padrão n8n): auth básica, User-Agent e SSL configurável."""
    if resource.startswith("http://") or resource.startswith("https://"):
        url = resource
    else:
        path = resource if resource.startswith("/") else f"/{resource}"
        url = f"{wp_base_url()}/wp-json/wp/v2{path}"

    max_retries = max(1, retries or as_int(get_env_or_variable("WP_MAX_RETRIES", "3"), 3))
    timeout_seconds = timeout or as_int(get_env_or_variable("WP_TIMEOUT_SECONDS", "30"), 30)
    backoff_base = max(1, as_int(get_env_or_variable("WP_RETRY_BACKOFF_SECONDS", "2"), 2))
    verify_ssl = wp_verify_ssl()
    req_headers = wp_headers(headers, include_auth=include_auth)

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.request(
                method=method.upper(),
                url=url,
                headers=req_headers,
                params=params,
                json=json_payload,
                data=data,
                timeout=timeout_seconds,
                verify=verify_ssl,
            )
            if 200 <= response.status_code < 300:
                return response

            body_preview = (response.text or "")[:250]
            retriable_status = response.status_code in {408, 425, 429, 500, 502, 503, 504}
            error = AirflowException(
                f"WordPress {method.upper()} {url} falhou com status {response.status_code}. "
                f"body={body_preview}"
            )
            if not retriable_status:
                raise error
            last_error = error
        except requests.RequestException as exc:
            last_error = exc

        if attempt < max_retries:
            logging.warning(
                "WordPress request tentativa %s/%s falhou. method=%s url=%s erro=%s",
                attempt,
                max_retries,
                method.upper(),
                url,
                last_error,
            )
            time.sleep(min(backoff_base * attempt, 15))

    raise AirflowException(
        f"WordPress indisponível após {max_retries} tentativas. "
        f"method={method.upper()} url={url} erro_final={last_error}"
    )


def wp_healthcheck() -> None:
    """Replica validação de credencial do n8n (GET /users)."""
    wp_request("GET", "/users", params={"per_page": 1, "_fields": "id"}, timeout=20, retries=2)


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


def remove_external_links(content: str, internal_domain: str = "ups1tride.com") -> str:
    """Remove links externos do HTML, mantendo apenas o texto âncora."""

    def replace_link(match):
        href = match.group(1)
        text = match.group(2)
        if href.startswith("/") or internal_domain in href:
            return match.group(0)
        logging.warning(f"Link externo removido: {href}")
        return text

    return re.sub(
        r'<a\s+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        replace_link,
        content or "",
        flags=re.IGNORECASE | re.DOTALL,
    )


def validate_content_hallucination(content: str, main_doc: dict) -> list[str]:
    """
    Verifica sinais de alucinação no conteúdo gerado.
    Retorna lista de alertas (não bloqueia publicação, apenas loga).
    """
    alerts = []

    fake_quotes = re.findall(r'"([^"]{40,})"', content or "")
    if fake_quotes:
        alerts.append(f"Possível declaração inventada detectada: {len(fake_quotes)} trecho(s) entre aspas longas.")

    speculation_phrases = [
        "deve ser",
        "provavelmente",
        "segundo fontes",
        "soube apurar",
        "segundo apurou",
        "tudo indica",
        "ao que tudo indica",
        "se confirmar",
        "negociações avançadas",
        "a caminho",
    ]
    content_lower = (content or "").lower()
    for phrase in speculation_phrases:
        if phrase.lower() in content_lower:
            alerts.append(f"Expressão especulativa detectada: '{phrase}'")

    external_links = re.findall(
        r'href=["\']https?://(?!ups1tride\.com)[^"\']+["\']',
        content or "",
        flags=re.IGNORECASE,
    )
    if external_links:
        alerts.append(f"Links externos detectados: {external_links}")

    for alert in alerts:
        logging.warning("[ANTI-ALUCINAÇÃO] club=%s | %s", main_doc.get("club_id", "?"), alert)

    return alerts


def enforce_club_page_links(content: str, club_page_url: str) -> str:
    """Força todos os links internos a usarem a club_page_url do clube."""

    def replace_link(match):
        text = match.group(2)
        return f'<a href="{club_page_url}">{text}</a>'

    return re.sub(
        r'<a\s+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        replace_link,
        content or "",
        flags=re.IGNORECASE | re.DOTALL,
    )


def insert_single_article_figure(content: str, figure: str) -> str:
    content = content or ""

    # Passo 1: remover marcadores duplicados, manter só o primeiro.
    first_marker_pos = content.find("<!--IMAGE_HERE-->")
    if first_marker_pos != -1:
        before = content[: first_marker_pos + len("<!--IMAGE_HERE-->")]
        after = content[first_marker_pos + len("<!--IMAGE_HERE-->") :].replace("<!--IMAGE_HERE-->", "")
        content = before + after
        # Passo 2: substituir o único marcador pela figura.
        content = content.replace("<!--IMAGE_HERE-->", figure, 1)
    elif re.search(r"</h1>", content, flags=re.IGNORECASE):
        content = re.sub(r"</h1>", f"</h1>\n{figure}", content, count=1, flags=re.IGNORECASE)
    else:
        content = f"{figure}\n{content}"

    # Passo 3: verificar e remover figuras duplicadas no HTML final.
    figures = re.findall(
        r'<figure[^>]*class=["\']article-image["\'][^>]*>.*?</figure>',
        content,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if len(figures) > 1:
        logging.warning("Múltiplas figuras detectadas (%s). Removendo excedentes.", len(figures))
        for extra_figure in figures[1:]:
            content = content.replace(extra_figure, "", 1)

    if not re.search(r'<figure[^>]*class=["\']article-image["\']', content, flags=re.IGNORECASE):
        content = f"{figure}\n{content}"

    return content


def insert_mandatory_club_link(content: str, club_id: str) -> str:
    club_name = CLUBS[club_id]["name"]
    page_url = CLUBS[club_id]["page_url"]
    link_html = (
        f'<p>Acompanhe todas as notícias do {club_name} em '
        f'<a href="{page_url}">nossa página dedicada ao clube</a>.</p>'
    )
    if re.search(
        r'<a\s+href=["\']'
        + re.escape(page_url)
        + r'["\'][^>]*>\s*nossa página dedicada ao clube\s*</a>',
        content or "",
        flags=re.IGNORECASE | re.DOTALL,
    ):
        return content

    content = content or ""
    last_p = content.lower().rfind("</p>")
    last_div = content.lower().rfind("</div>")
    insert_pos = max(last_p, last_div)
    if insert_pos != -1:
        return f"{content[:insert_pos]}{link_html}\n{content[insert_pos:]}"
    return f"{content}\n{link_html}"


def increase_content_font_size(content: str) -> str:
    content = content or ""
    if re.search(r'style=["\'][^"\']*font-size', content, flags=re.IGNORECASE):
        return content
    return f'<div class="article-body" style="font-size: 1.08rem; line-height: 1.8;">\n{content}\n</div>'


def load_used_doc_ids() -> list[str]:
    raw = Variable.get(USED_DOCS_VARIABLE, default_var="[]")
    try:
        parsed = json.loads(raw)
    except Exception:
        parsed = []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip() != ""]


def get_club_page_id(club_id: str) -> int | None:
    """Busca o ID da página do clube no WordPress pelo slug."""
    slug = CLUBS[club_id]["page_url"].rstrip("/").split("/")[-1]
    try:
        response = wp_request(
            "GET",
            "/pages",
            params={"slug": slug, "_fields": "id,slug,link", "per_page": 1},
            timeout=10,
            retries=2,
        )
        pages = response.json() or []
        if pages:
            return pages[0].get("id")
    except Exception as exc:
        logging.warning("Falha ao buscar página do clube slug=%s erro=%s", slug, exc)

    logging.warning("Página do clube não encontrada para slug: %s", slug)
    return None


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
    club_id = extract_doc_club_id(doc) or (doc.get("club_id") or "")
    return {
        "club_id": club_id,
        "title": (doc.get("title") or doc.get("headline") or "").strip(),
        "summary": (doc.get("summary") or doc.get("description") or "").strip(),
        "snippet": (doc.get("snippet") or doc.get("excerpt") or "").strip(),
        "url": extract_doc_url(doc),
        "published_at": (doc.get("published_at") or "").strip(),
        "source_name": str(source).strip(),
        "_id": (doc.get("_id") or "").strip(),
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
    resource = f"/{endpoint}"

    for include_auth in (True, False):
        try:
            response = wp_request(
                "GET",
                resource,
                params={"per_page": 100, "slug": slug, "_fields": "id,slug,name"},
                timeout=20,
                include_auth=include_auth,
                retries=2,
            )
            terms = response.json() or []
            if terms:
                return terms[0].get("id")
        except Exception as exc:
            logging.error(
                "Falha ao buscar term endpoint=%s slug=%s include_auth=%s erro=%s",
                endpoint,
                slug,
                include_auth,
                exc,
            )

    try:
        create_response = wp_request(
            "POST",
            resource,
            json_payload={"name": name, "slug": slug},
            timeout=20,
            include_auth=True,
            retries=2,
        )
        created = create_response.json() or {}
        return created.get("id")
    except Exception as exc:
        logging.error("Erro ao criar term endpoint=%s slug=%s erro=%s", endpoint, slug, exc)

    return None


def get_or_create_club_category(club_id: str) -> int:
    """
    Busca a categoria do clube no WordPress pelo slug.
    Se não existir, cria com o nome e slug corretos.
    Retorna o category_id.
    Lança Exception se não conseguir criar ou encontrar.
    """
    slug = CLUB_PAGE_SLUGS[club_id]
    name = CLUBS[club_id]["name"]
    base_url = f"{wp_base_url()}/wp-json/wp/v2/categories"
    headers = wp_headers()

    # 1. Tentar buscar pelo slug exato.
    resp = requests.get(
        f"{base_url}?slug={slug}&per_page=1&_fields=id,slug,name,link",
        headers=headers,
        timeout=10,
        verify=wp_verify_ssl(),
    )
    if resp.status_code == 200:
        results = resp.json()
        if results:
            cat_id = results[0]["id"]
            logging.info("Categoria encontrada: %s (id=%s, slug=%s)", name, cat_id, slug)
            return cat_id

    # 2. Tentar buscar pelo nome exato.
    resp2 = requests.get(
        f"{base_url}?search={name}&per_page=10&_fields=id,slug,name",
        headers=headers,
        timeout=10,
        verify=wp_verify_ssl(),
    )
    if resp2.status_code == 200:
        for cat in resp2.json():
            if cat.get("slug") == slug or cat.get("name", "").upper() == name.upper():
                cat_id = cat["id"]
                logging.info("Categoria encontrada por nome: %s (id=%s)", name, cat_id)
                return cat_id

    # 3. Criar a categoria.
    payload = {"name": name, "slug": slug}
    resp3 = requests.post(base_url, headers=headers, json=payload, timeout=10, verify=wp_verify_ssl())

    if resp3.status_code in (200, 201):
        cat_id = (resp3.json() or {}).get("id")
        if cat_id:
            logging.info("Categoria criada: %s (id=%s, slug=%s)", name, cat_id, slug)
            return cat_id

    # 4. Conflito de slug com term_id já existente.
    error_data = resp3.json() if resp3.content else {}
    existing_id = (error_data.get("additional_data", {}) or {}).get("term_id") or (
        error_data.get("data", {}) or {}
    ).get("term_id")
    if existing_id:
        logging.info("Categoria já existia (conflito): %s (id=%s)", name, existing_id)
        return existing_id

    raise Exception(
        f"Falha ao criar/encontrar categoria para {club_id}: "
        f"status={resp3.status_code} body={(resp3.text or '')[:200]}"
    )


def publish_post_to_club_page(
    club_id: str,
    title: str,
    content: str,
    slug: str,
    tag_ids: list[int],
    media_id: int | None,
) -> dict:
    """
    Publica o post no WordPress vinculado EXCLUSIVAMENTE
    à categoria do clube, garantindo que aparece na página do clube.
    """
    category_id = get_or_create_club_category(club_id)

    body = {
        "title": title,
        "content": content,
        "slug": slug,
        "status": "publish",
        "categories": [category_id],
        "tags": tag_ids,
    }
    if media_id:
        body["featured_media"] = media_id

    headers = wp_headers()

    resp = requests.post(
        f"{wp_base_url()}/wp-json/wp/v2/posts",
        headers=headers,
        json=body,
        timeout=30,
        verify=wp_verify_ssl(),
    )

    if resp.status_code not in (200, 201):
        raise Exception(
            f"Falha ao publicar post para {club_id}: "
            f"status={resp.status_code} body={(resp.text or '')[:300]}"
        )

    result = resp.json() or {}
    wp_post_id = result.get("id")
    wp_post_link = result.get("link", "")
    wp_post_status = result.get("status", "")

    if wp_post_status != "publish":
        raise Exception(
            f"Post criado mas não publicado para {club_id}: "
            f"status={wp_post_status} id={wp_post_id}"
        )

    logging.info(
        "Post publicado: clube=%s | id=%s | categoria=%s | link=%s",
        club_id,
        wp_post_id,
        category_id,
        wp_post_link,
    )

    return {
        "wp_post_id": wp_post_id,
        "wp_post_link": wp_post_link,
        "category_id": category_id,
        "club_id": club_id,
    }


def verify_post_on_club_page(wp_post_id: int, club_id: str) -> bool:
    """
    Confirma que o post publicado está associado à categoria do clube.
    """
    headers = wp_headers()
    headers.pop("Content-Type", None)

    resp = requests.get(
        f"{wp_base_url()}/wp-json/wp/v2/posts/{wp_post_id}?_fields=id,categories,link",
        headers=headers,
        timeout=10,
        verify=wp_verify_ssl(),
    )
    if resp.status_code != 200:
        logging.warning("Não foi possível verificar post %s", wp_post_id)
        return False

    post_data = resp.json() or {}
    category_id = get_or_create_club_category(club_id)
    categories = post_data.get("categories", [])

    if category_id in categories:
        logging.info(
            "VERIFICADO: post %s está na página do clube %s | link=%s",
            wp_post_id,
            club_id,
            post_data.get("link", ""),
        )
        return True

    logging.error(
        "FALHA NA VERIFICAÇÃO: post %s NÃO está na categoria %s do clube %s. Categorias encontradas: %s",
        wp_post_id,
        category_id,
        club_id,
        categories,
    )
    return False


def find_wp_post_by_slug(slug: str) -> dict | None:
    """Busca post por slug para recuperação após falha de conexão no publish."""
    if not slug:
        return None
    try:
        response = wp_request(
            "GET",
            "/posts",
            params={"per_page": 5, "slug": slug, "_fields": "id,slug,status"},
            timeout=20,
            retries=2,
        )
        posts = response.json() or []
        if posts:
            return posts[0]
    except Exception as exc:
        logging.warning("Falha ao recuperar post por slug=%s erro=%s", slug, exc)
    return None


def build_state(today: str) -> dict:
    return {
        "date": today,
        "posted": {},
        "last_picked_clubs": [],
        "last_run_at": None,
        "used_doc_ids": [],
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
    state.setdefault("used_doc_ids", [])

    used_doc_ids = load_used_doc_ids()
    state["used_doc_ids"] = used_doc_ids

    ti.xcom_push(key="state", value=state)
    ti.xcom_push(key="today", value=today)
    ti.xcom_push(key="run_id", value=run_id)
    ti.xcom_push(key="state_key", value=state_key)
    ti.xcom_push(key="used_doc_ids", value=used_doc_ids)

    logging.info(
        "State carregado para %s com %s clubes já postados e %s docs usados.",
        today,
        len(state["posted"]),
        len(state["used_doc_ids"]),
    )
    return state


def task_fetch_elasticsearch(**kwargs):
    ti = kwargs["ti"]
    state = ti.xcom_pull(task_ids="load_state", key="state") or {}
    already_posted_doc_ids = state.get("used_doc_ids") or []
    url = f"{ES_HOST.rstrip('/')}/{ES_INDEX}/_search"
    docs_by_club: dict[str, list[dict]] = {}

    for club_id, club_cfg in CLUBS.items():
        must_not = [{"ids": {"values": already_posted_doc_ids}}]

        payload = {
            "size": 10,
            "sort": [{"published_at": {"order": "desc"}}],
            "query": {
                "bool": {
                    "must": [
                        {
                            "bool": {
                                "should": [
                                    {"term": {"club_id": club_id}},
                                    {"match": {"club_name": club_cfg["name"]}},
                                ],
                                "minimum_should_match": 1,
                            }
                        }
                    ],
                    "must_not": must_not,
                }
            },
        }

        try:
            response = requests.post(url, json=payload, timeout=20)
            if not response.ok:
                logging.warning(
                    "Consulta ES falhou club_id=%s status=%s body=%s",
                    club_id,
                    response.status_code,
                    (response.text or "")[:400],
                )
            response.raise_for_status()

            data = response.json() or {}
            hits = (((data.get("hits") or {}).get("hits")) or [])
            docs = []
            for item in hits:
                source = item.get("_source") or {}
                if not isinstance(source, dict):
                    continue
                source["_id"] = item.get("_id")
                source["club_id"] = source.get("club_id") or club_id
                docs.append(source)

            docs = sorted(docs, key=lambda item: parse_published_at(item.get("published_at")), reverse=True)

            if len(docs) < 1:
                logging.warning("club_id=%s sem documentos novos suficientes no Elasticsearch.", club_id)
                continue

            docs_by_club[club_id] = docs
        except Exception as exc:
            logging.warning("Falha na consulta ES para club_id=%s erro=%s", club_id, exc)

    ti.xcom_push(key="es_docs", value=docs_by_club)
    return docs_by_club


def task_fetch_wp_recent_posts(**kwargs):
    ti = kwargs["ti"]

    posts_out = []
    recent_club_ids = []

    try:
        wp_healthcheck()
        response = wp_request(
            "GET",
            "/posts",
            params={
                "per_page": 10,
                "_embed": 1,
                "_fields": "id,slug,title,_embedded",
            },
            timeout=20,
        )
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
        raise AirflowFailException(
            "WordPress indisponível para leitura da API. "
            f"Erro final após retries: {exc}"
        )

    ti.xcom_push(key="recent_posts", value=posts_out)
    ti.xcom_push(key="recent_club_ids", value=recent_club_ids)
    return {"recent_posts": posts_out, "recent_club_ids": recent_club_ids}


def task_select_clubs(**kwargs):
    ti = kwargs["ti"]
    state = ti.xcom_pull(task_ids="load_state", key="state") or build_state(get_today_fortaleza())
    docs_by_club = ti.xcom_pull(task_ids="fetch_elasticsearch", key="es_docs") or {}
    if not isinstance(docs_by_club, dict):
        docs_by_club = {}

    valid_docs_by_club = {
        club_id: sorted(docs, key=lambda item: parse_published_at(item.get("published_at")), reverse=True)
        for club_id, docs in docs_by_club.items()
        if isinstance(docs, list) and len(docs) > 0
    }

    if len(valid_docs_by_club) < 3:
        logging.warning(
            "Documentos insuficientes por clube. clubes_disponiveis=%s",
            sorted(valid_docs_by_club.keys()),
        )
        raise AirflowException(
            f"Não há clubes suficientes com documentos no ES. Encontrados={len(valid_docs_by_club)}, necessário=3"
        )

    posted_today = set((state.get("posted") or {}).keys())
    last_picked = state.get("last_picked_clubs") or []

    all_clubs_with_docs = list(valid_docs_by_club.keys())
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
                "club_page_url": club_data["page_url"],
                "docs_sorted": valid_docs_by_club.get(club_id, []),
                "data_contract": DATA_CONTRACT,
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
    data_contract = club.get("data_contract") or DATA_CONTRACT

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
        used_doc_ids = set(state.get("used_doc_ids") or [])
        available_docs = [doc for doc in docs_sorted if str(doc.get("_id") or "") not in used_doc_ids]
        if not available_docs:
            logging.warning("club_id=%s todos os docs disponíveis já foram utilizados. Pulando clube.", club_id)
            result = fail("ETAPA_A", "Nenhum documento disponível para o clube")
            ti.xcom_push(key="result", value=result)
            return result
        main_doc = available_docs[0]
        main_doc_id = str(main_doc.get("_id") or "")
        related_docs = available_docs[1:6]
        main_compact = compact_doc_for_prompt(main_doc)
        related_compact = [
            {
                "title": compact_doc_for_prompt(doc).get("title", ""),
                "summary": compact_doc_for_prompt(doc).get("summary", ""),
                "snippet": compact_doc_for_prompt(doc).get("snippet", ""),
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
            "regras_iniciais": [
                "Escreva SOMENTE com base nos dados de main_doc e related_docs fornecidos.",
                "Nunca inventar placares, contratações, demissões, escalações ou declarações que não estejam nos dados recebidos.",
                "Se um dado não estiver confirmado nos documentos, não mencione.",
                "Nunca incluir links externos ou citar outros sites no corpo do artigo.",
                "Todos os links internos devem apontar exclusivamente para páginas do próprio site usando club_page_url fornecido.",
                "O artigo deve ter entre 750 e 1200 palavras.",
                "Inserir exatamente UM marcador <!--IMAGE_HERE--> no melhor ponto do texto. Não inserir mais de um marcador. Não inserir a tag <img> diretamente.",
            ],
            "instrucao": "Retorne APENAS JSON válido no formato solicitado.",
            "regras": {
                "palavras": "Obrigatório entre 750 e 1200 palavras.",
                "foco": "90% na notícia principal e 10% de contexto com relacionadas.",
                "factual": "Não inventar dados; usar apenas os dados fornecidos.",
                "links": "Não usar links externos no corpo; usar apenas club_page_url em links internos.",
                "imagem": "Inserir exatamente um marcador literal <!--IMAGE_HERE--> no ponto da imagem, sem tag <img>.",
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
                "data_contract": data_contract,
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
                        "Você é um jornalista esportivo especializado em futebol brasileiro.\n"
                        "Escreva apenas com base nos dados fornecidos em main_doc e related_docs.\n"
                        "Nunca invente fatos, resultados, nomes de jogadores, escalações ou\n"
                        "transferências que não estejam explicitamente nos dados recebidos.\n"
                        "Se os dados forem insuficientes para um artigo completo, use apenas\n"
                        "o que está disponível e sinalize a limitação com linguagem jornalística\n"
                        "apropriada (ex: 'segundo informações iniciais', 'conforme apurado').\n\n"
                        "Identidade visual dos clubes — respeitar rigorosamente:\n"
                        "- Flamengo: vermelho e preto\n"
                        "- Palmeiras: verde e branco\n"
                        "- Corinthians: preto e branco\n"
                        "- Vasco: preto e branco (cruz de Malta)\n"
                        "- Santos: branco e preto\n"
                        "- São Paulo: tricolor (vermelho, preto e branco)\n"
                        "- Cruzeiro: azul e branco\n"
                        "- Atlético-MG: preto e branco (Galo)\n"
                        "- Fortaleza: azul, vermelho e preto (Leão do Pici)\n"
                        "- Bahia: azul e vermelho (Esquadrão de Aço)\n"
                        "- Grêmio: azul, preto e branco (Tricolor Gaúcho)\n\n"
                        "Nunca mencionar cores, símbolos ou características visuais de um clube\n"
                        "em um artigo de outro clube. Nunca confundir rivais ou misturar\n"
                        "identidades de times diferentes no mesmo texto.\n\n"
                        "Retorne APENAS JSON válido no formato solicitado. O campo CONTENT deve\n"
                        "ser HTML completo, sem markdown."
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
        if not slug.startswith(f"{club_id}-"):
            slug = normalize_slug(f"{club_id}-{slug}")

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

    # ETAPA C2 — Anti-alucinação e limpeza de links externos
    try:
        validate_content_hallucination(content, main_compact)
        content = remove_external_links(content)
    except Exception as exc:
        result = fail("ETAPA_C2", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA D — Gerar imagem com DALL-E 3
    image_url = None
    try:
        main_summary = main_compact.get("summary") or ""
        club_colors = CLUB_COLORS.get(club_id, "cores oficiais do clube")
        image_prompt = (
            "Fotojornalismo esportivo ultrarrealista, futebol brasileiro profissional. "
            f"Clube: {club_name}. "
            f"Cores predominantes: {club_colors}. "
            f"Contexto: {title}. {main_summary}. "
            "Sem escudos, logos ou símbolos de clubes. Sem texto. Sem watermark. "
            "Iluminação natural, câmera profissional 35mm, estilo editorial esportivo."
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
    media_url = image_url
    try:
        image_bytes_response = requests.get(image_url, timeout=30)
        image_bytes_response.raise_for_status()
        image_bytes = image_bytes_response.content

        filename = f"dalle-{club_id}-{int(time.time())}.png"
        media_headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type": "image/png",
        }

        upload_response = wp_request(
            "POST",
            "/media",
            headers=media_headers,
            data=image_bytes,
            timeout=60,
            retries=3,
        )
        media_json = upload_response.json() or {}
        media_id = media_json.get("id")
        media_url = media_json.get("source_url") or image_url
        if not media_id:
            raise AirflowException("Resposta de upload sem media_id")
    except Exception as exc:
        media_id = None
        media_url = fallback_image_url or image_url
        logging.warning(
            "club_id=%s ETAPA_E sem upload de mídia; seguindo sem featured_media. erro=%s",
            club_id,
            exc,
        )

    # ETAPA F — Inserir imagem no CONTENT
    try:
        if media_url:
            figure = (
                '<figure class="article-image">\n'
                f'  <img src="{media_url}" alt="{title}"/>\n'
                "  <figcaption>Imagem: OpenAI DALL-E 3</figcaption>\n"
                "</figure>"
            )
            content = insert_single_article_figure(content, figure)
        else:
            content = content.replace("<!--IMAGE_HERE-->", "")

        figure_count = len(
            re.findall(
                r'<figure[^>]*class=["\']article-image["\'][^>]*>.*?</figure>',
                content or "",
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        if figure_count != 1:
            result = fail("ETAPA_F", f"HTML final inválido: esperada 1 figure article-image, encontrado {figure_count}")
            ti.xcom_push(key="result", value=result)
            return result
    except Exception as exc:
        result = fail("ETAPA_F", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA F2 — Sanitização final de links internos e acabamento de conteúdo
    try:
        content = enforce_club_page_links(content, club_page_url)
        content = insert_mandatory_club_link(content, club_id)
        content = increase_content_font_size(content)
    except Exception as exc:
        result = fail("ETAPA_F2", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA G — Resolver categoria do clube no WordPress
    try:
        category_id = get_or_create_club_category(club_id)
        if not category_id:
            result = fail("ETAPA_G", "Categoria do clube não encontrada/criada")
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
            existing_resp = wp_request(
                "GET",
                "/tags",
                params={"per_page": 100, "slug": slugs_csv, "_fields": "id,slug,name"},
                timeout=20,
            )
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
            logging.warning("club_id=%s ETAPA_H sem tags resolvidas; publicando sem tags", club_id)
    except Exception as exc:
        result = fail("ETAPA_H", str(exc))
        ti.xcom_push(key="result", value=result)
        return result

    # ETAPA I — Publicar post no WordPress
    try:
        post_title = title
        final_content = content
        post_slug = slug

        published = publish_post_to_club_page(
            club_id=club_id,
            title=post_title,
            content=final_content,
            slug=post_slug,
            tag_ids=tag_ids,
            media_id=media_id,
        )
        wp_post_id = published["wp_post_id"]
        if not wp_post_id:
            raise AirflowException("Resposta sem id do post")

        try:
            verify_post_on_club_page(wp_post_id, club_id)
        except Exception as verify_exc:
            logging.error("Falha na verificação do post %s no clube %s: %s", wp_post_id, club_id, verify_exc)

        result = {
            "club_id": club_id,
            "status": "success",
            "wp_post_id": wp_post_id,
            "main_doc_id": main_doc_id,
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
    state.setdefault("used_doc_ids", [])

    new_used_doc_ids = []

    for result in results:
        if result.get("status") == "success" and result.get("club_id"):
            state["posted"][result["club_id"]] = {
                "at": now_fortaleza_iso(),
                "wp_post_id": result.get("wp_post_id"),
            }
            main_doc_id = str(result.get("main_doc_id") or "").strip()
            if main_doc_id:
                new_used_doc_ids.append(main_doc_id)

    used_docs = load_used_doc_ids()
    used_docs = (used_docs + new_used_doc_ids)[-500:]
    state["used_doc_ids"] = used_docs
    Variable.set(USED_DOCS_VARIABLE, json.dumps(used_docs, ensure_ascii=False))

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
        raise AirflowFailException("Nenhum artigo publicado neste disparo")

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
