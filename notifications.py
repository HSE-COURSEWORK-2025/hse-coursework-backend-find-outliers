import datetime
from typing import List, Optional

import httpx
from pydantic import BaseModel
from settings import settings


class EmailNotificationRequest(BaseModel):
    to_email: str
    subject: str
    message: str


class Notification(BaseModel):
    id: int
    for_email: str
    time: datetime.datetime
    notification_text: str
    checked: bool


class NotificationsAPIClient:
    """
    Асинхронный клиент для работы с Notifications API.
    """

    def __init__(
        self,
        base_url: str = settings.NOTIFICATIONS_API_BASE_URL,
        token: Optional[str] = None,
        timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/")
        self._headers = {}
        if token:
            self._headers["Authorization"] = f"Bearer {token}"
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=self._headers,
            timeout=timeout,
        )

    async def send_email(
        self,
        to_email: str,
        subject: str,
        message: str,
    ) -> dict:
        """
        Отправка email-уведомления и запись в БД.
        """
        payload = EmailNotificationRequest(
            to_email=to_email, subject=subject, message=message
        ).dict()
        resp = await self._client.post("/send_email", json=payload)
        resp.raise_for_status()
        return resp.json()

    async def get_unchecked_notifications(self) -> List[Notification]:
        """
        Получить все непрочитанные уведомления текущего пользователя
        и одновременно пометить их как прочитанные.
        """
        resp = await self._client.get("/get_unchecked_notifications")
        resp.raise_for_status()
        data = resp.json()
        return [Notification.parse_obj(item) for item in data]

    async def get_all_notifications(self) -> List[Notification]:
        """
        Получить все уведомления текущего пользователя (прочитанные и нет).
        """
        resp = await self._client.get("/get_all_notifications")
        resp.raise_for_status()
        data = resp.json()
        return [Notification.parse_obj(item) for item in data]

    async def close(self):
        """Закрыть httpx-клиент."""
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()


notifications_api = NotificationsAPIClient()
