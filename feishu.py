import os
import requests
from datetime import datetime


def send_to_feishu(date: str, text: str) -> bool:
    """
    Send a message to Feishu via webhook.

    Args:
        date: The date string to include in the message
        text: Markdown formatted text content (supports links, headings, etc.)

    Returns:
        bool: True if successful, False otherwise
    """
    webhook_url = os.getenv("FEISHU_WEBHOOK_URL")
    if not webhook_url:
        print("Error: FEISHU_WEBHOOK_URL not set in environment")
        return False

    payload = {
        "msg_type": "text",
        "content": {
            "date": date,
            "text": text
        }
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"Feishu message sent successfully at {datetime.now().isoformat()}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to send Feishu message: {e}")
        return False


def send_feed_summary_to_feishu(feed_items: list, content_max_length: int = 500) -> bool:
    """
    Send all feed items to Feishu in a single message.

    Args:
        feed_items: List of feed item dictionaries with 'title', 'link', 'date', etc.
        content_max_length: Maximum length for content field (default: 500)

    Returns:
        bool: True if successful, False otherwise
    """
    if not feed_items:
        print("No feed items to send")
        return True

    today = datetime.now().strftime("%Y-%m-%d")

    lines = [f"📰 RSS Feed 摘要 ({today})", ""]

    for item in feed_items:
        title = item.get("title", "无标题")
        link = item.get("link", "")

        if link:
            lines.append(f"🔗 [{title}]({link})")
        else:
            lines.append(f"{title}")

        lines.append("")

    text_content = "\n".join(lines)

    return send_to_feishu(today, text_content)
