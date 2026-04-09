import datetime
import logging
import os

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_READER_DATABASE_ID = os.getenv("NOTION_READER_DATABASE_ID")
NOTION_FEEDS_DATABASE_ID = os.getenv("NOTION_FEEDS_DATABASE_ID")

NOTION_API_VERSION = "2022-06-28"
NOTION_BASE_URL = "https://api.notion.com/v1"
_MAX_BLOCKS_PER_REQUEST = 100
_REQUEST_TIMEOUT = 30
_ARCHIVE_AFTER_DAYS = 30


def _get_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": NOTION_API_VERSION,
    }


def _query_database_with_pagination(
    database_id: str, payload: dict
) -> list[dict]:
    url = f"{NOTION_BASE_URL}/databases/{database_id}/query"
    all_results: list[dict] = []
    has_more = True
    start_cursor: str | None = None

    while has_more:
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            response = requests.post(
                url, headers=_get_headers(), json=payload, timeout=_REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as err:
            logger.error("Error querying database %s: %s", database_id, err)
            break

        all_results.extend(data.get("results", []))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")

    return all_results


def get_feed_urls_from_notion() -> list[dict]:
    payload = {
        "filter": {
            "property": "Rss地址",
            "url": {"is_not_empty": True},
        }
    }

    results = _query_database_with_pagination(NOTION_FEEDS_DATABASE_ID, payload)

    feeds: list[dict] = []
    for item in results:
        props = item.get("properties", {})
        title_prop = props.get("名称", {}).get("title", [])
        link_prop = props.get("Rss地址", {}).get("url")

        title = title_prop[0].get("plain_text", "") if title_prop else ""
        feeds.append({"title": title, "feedUrl": link_prop})

    return feeds


def get_existing_items_since(days: int = 5) -> tuple[set[str], set[str]]:
    since_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days)

    payload = {
        "filter": {
            "timestamp": "created_time",
            "created_time": {"on_or_after": since_date.isoformat()},
        }
    }

    results = _query_database_with_pagination(NOTION_READER_DATABASE_ID, payload)

    titles: set[str] = set()
    links: set[str] = set()

    for item in results:
        props = item.get("properties", {})
        title_parts = props.get("名称", {}).get("title", [])
        if title_parts:
            titles.add(title_parts[0].get("plain_text", ""))
        link_val = props.get("听原文", {}).get("url")
        if link_val:
            links.add(link_val)

    return titles, links


def add_feed_item_to_notion(notion_item: dict) -> bool:
    title = notion_item.get("title", "")
    link = notion_item.get("link", "")
    content: list[dict] = notion_item.get("content", [])
    published_date = notion_item.get("published_date")

    first_chunk = content[:_MAX_BLOCKS_PER_REQUEST]
    remaining_chunks = [
        content[i:i + _MAX_BLOCKS_PER_REQUEST]
        for i in range(_MAX_BLOCKS_PER_REQUEST, len(content), _MAX_BLOCKS_PER_REQUEST)
    ]

    properties = {
        "名称": {"title": [{"text": {"content": title}}]},
        "听原文": {"url": link},
        "处理状态": {"status": {"name": "收录中"}},
    }

    if published_date:
        properties["发布日期"] = {
            "date": {"start": published_date}
        }

    payload = {
        "parent": {"database_id": NOTION_READER_DATABASE_ID},
        "properties": properties,
        "children": first_chunk,
    }

    try:
        response = requests.post(
            f"{NOTION_BASE_URL}/pages",
            headers=_get_headers(),
            json=payload,
            timeout=_REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        page_id = response.json().get("id")
    except requests.exceptions.RequestException as err:
        logger.error("Error adding feed item to Notion: %s", err)
        return False

    for chunk in remaining_chunks:
        try:
            requests.patch(
                f"{NOTION_BASE_URL}/blocks/{page_id}/children",
                headers=_get_headers(),
                json={"children": chunk},
                timeout=_REQUEST_TIMEOUT,
            )
        except requests.exceptions.RequestException as err:
            logger.error("Error appending blocks to page %s: %s", page_id, err)

    return True


def delete_old_unread_feed_items_from_notion() -> None:
    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=_ARCHIVE_AFTER_DAYS)

    payload = {
        "filter": {
            "and": [
                {
                    "timestamp": "created_time",
                    "created_time": {"on_or_before": cutoff.isoformat()},
                },
                {
                    "property": "处理状态",
                    "status": {"equals": "收录中"},
                },
            ]
        }
    }

    results = _query_database_with_pagination(NOTION_READER_DATABASE_ID, payload)
    archived = 0

    for item in results:
        page_id = item.get("id")
        try:
            requests.patch(
                f"{NOTION_BASE_URL}/pages/{page_id}",
                headers=_get_headers(),
                json={"archived": True},
                timeout=_REQUEST_TIMEOUT,
            )
            archived += 1
        except requests.exceptions.RequestException as err:
            logger.error("Error archiving page %s: %s", page_id, err)

    if archived:
        logger.info("Archived %d old unread items", archived)