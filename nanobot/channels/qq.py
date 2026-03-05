"""QQ channel implementation using botpy SDK."""

import asyncio
import hashlib
from collections import deque
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import QQConfig

try:
    import botpy
    from botpy.message import C2CMessage

    QQ_AVAILABLE = True
except ImportError:
    QQ_AVAILABLE = False
    botpy = None
    C2CMessage = None

if TYPE_CHECKING:
    from botpy.message import C2CMessage


def _make_bot_class(channel: "QQChannel") -> "type[botpy.Client]":
    """Create a botpy Client subclass bound to the given channel."""
    intents = botpy.Intents(public_messages=True, direct_message=True)

    class _Bot(botpy.Client):
        def __init__(self):
            # Disable botpy's file log — nanobot uses loguru; default "botpy.log" fails on read-only fs
            super().__init__(intents=intents, ext_handlers=False)

        async def on_ready(self):
            logger.info("QQ bot ready: {}", self.robot.name)

        async def on_c2c_message_create(self, message: "C2CMessage"):
            await channel._on_message(message)

        async def on_direct_message_create(self, message):
            await channel._on_message(message)

    return _Bot


class QQChannel(BaseChannel):
    """QQ channel using botpy SDK with WebSocket connection."""

    name = "qq"

    def __init__(self, config: QQConfig, bus: MessageBus, groq_api_key: str = ""):
        super().__init__(config, bus)
        self.config: QQConfig = config
        self.groq_api_key = groq_api_key
        self._client: "botpy.Client | None" = None
        self._processed_ids: deque = deque(maxlen=1000)
        self._msg_seq_counter: dict[str, int] = {}  # Track msg_seq for each msg_id

    async def start(self) -> None:
        """Start the QQ bot."""
        if not QQ_AVAILABLE:
            logger.error("QQ SDK not installed. Run: pip install qq-botpy")
            return

        if not self.config.app_id or not self.config.secret:
            logger.error("QQ app_id and secret not configured")
            return

        self._running = True
        BotClass = _make_bot_class(self)
        self._client = BotClass()

        logger.info("QQ bot started (C2C private message)")
        await self._run_bot()

    async def _run_bot(self) -> None:
        """Run the bot connection with auto-reconnect."""
        while self._running:
            try:
                await self._client.start(appid=self.config.app_id, secret=self.config.secret)
            except Exception as e:
                logger.warning("QQ bot error: {}", e)
            if self._running:
                logger.info("Reconnecting QQ bot in 5 seconds...")
                await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the QQ bot."""
        self._running = False
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
        logger.info("QQ bot stopped")

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through QQ."""
        if not self._client:
            logger.warning("QQ client not initialized")
            return
        try:
            # Use msg_id from received message for passive reply
            msg_id = msg.metadata.get("message_id")

            if msg_id:
                # Increment msg_seq for this msg_id to avoid deduplication
                self._msg_seq_counter[msg_id] = self._msg_seq_counter.get(msg_id, 0) + 1
                msg_seq = self._msg_seq_counter[msg_id]

                await self._client.api.post_c2c_message(
                    openid=msg.chat_id,
                    msg_type=0,
                    content=msg.content,
                    msg_id=msg_id,
                    msg_seq=msg_seq,
                )
            else:
                # Active message without msg_id
                await self._client.api.post_c2c_message(
                    openid=msg.chat_id,
                    msg_type=0,
                    content=msg.content,
                )
        except Exception as e:
            logger.error("Error sending QQ message: {}", e)

    async def _on_message(self, data: "C2CMessage") -> None:
        """Handle incoming message from QQ."""
        try:
            # Dedup by message ID
            if data.id in self._processed_ids:
                return
            self._processed_ids.append(data.id)

            author = data.author
            user_id = str(getattr(author, 'id', None) or getattr(author, 'user_openid', 'unknown'))
            content = (data.content or "").strip()

            # Check for attachments (images, voice, etc.)
            attachments = getattr(data, 'attachments', None)
            media_paths = []

            # Download and process media attachments
            if attachments:
                media_dir = Path.home() / ".nanobot" / "media"
                media_dir.mkdir(parents=True, exist_ok=True)

                for att in attachments:
                    try:
                        # Get attachment properties
                        url = getattr(att, 'url', '')
                        content_type = getattr(att, 'content_type', '')
                        filename = getattr(att, 'filename', '')

                        # Debug: log attachment details
                        logger.debug("Attachment - url: {}, content_type: {}, filename: {}", url, content_type, filename)

                        if not url:
                            logger.warning("Attachment has no URL: {}", att)
                            continue

                        # Determine media type and extension
                        if 'image' in content_type or any(url.lower().endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                            media_type = "image"
                            ext = self._get_safe_extension(url, content_type) or ".jpg"
                        elif 'audio' in content_type or 'voice' in content_type or any(url.lower().endswith(ext) for ext in ['.mp3', '.ogg', '.wav', '.m4a']):
                            media_type = "voice"
                            ext = self._get_safe_extension(url, content_type) or ".ogg"
                        elif 'video' in content_type or any(url.lower().endswith(ext) for ext in ['.mp4', '.avi', '.mov']):
                            media_type = "video"
                            ext = self._get_safe_extension(url, content_type) or ".mp4"
                        else:
                            media_type = "file"
                            ext = self._get_safe_extension(url, content_type) or ""

                        # Generate safe filename using hash of URL
                        url_hash = hashlib.md5(url.encode()).hexdigest()[:16]
                        file_path = media_dir / f"{url_hash}{ext}"

                        async with httpx.AsyncClient() as client:
                            response = await client.get(url, timeout=30.0)
                            response.raise_for_status()
                            file_path.write_bytes(response.content)

                        media_paths.append(str(file_path))
                        logger.info("Downloaded {} to {}", media_type, file_path)

                        # Add media description to content
                        if content:
                            content += f" [{media_type}: {file_path}]"
                        else:
                            content = f"[{media_type}: {file_path}]"

                    except Exception as e:
                        logger.error("Failed to download attachment: {}", e)
                        # Add error description to content
                        if content:
                            content += f" [{media_type}: download failed]"
                        else:
                            content = f"[{media_type}: download failed]"

            # Skip if no content and no media
            if not content:
                return

            await self._handle_message(
                sender_id=user_id,
                chat_id=user_id,
                content=content,
                media=media_paths,
                metadata={"message_id": data.id},
            )
        except Exception:
            logger.exception("Error handling QQ message")

    @staticmethod
    def _get_safe_extension(url: str, content_type: str) -> str:
        """Extract safe file extension from URL or content_type."""
        # Try to get from content_type first
        ext_map = {
            "image/jpeg": ".jpg",
            "image/jpg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "image/webp": ".webp",
            "audio/ogg": ".ogg",
            "audio/mpeg": ".mp3",
            "audio/mp4": ".m4a",
            "audio/amr": ".amr",
            "video/mp4": ".mp4",
            "voice": ".amr",  # QQ voice messages use AMR format
        }
        if content_type in ext_map:
            return ext_map[content_type]

        # Try to extract from URL
        try:
            path = url.split('?')[0]  # Remove query params
            path = path.split('/')[-1]  # Get filename only
            if '.' in path:
                ext = '.' + path.rsplit('.', 1)[-1].lower()
                # Ensure extension is safe (no path separators, reasonable length)
                if '/' not in ext and '\\' not in ext and len(ext) <= 10:
                    return ext
        except Exception:
            pass
        return ""
