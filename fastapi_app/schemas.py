from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class MessageRow(BaseModel):
    channel: Optional[str]
    message_id: Optional[int]
    message_text: Optional[str]
    message_date: Optional[datetime]
    views: Optional[int]
    has_media: Optional[bool]

class DetectionRow(BaseModel):
    channel: Optional[str]
    message_id: Optional[int]
    image_path: Optional[str]
    object: Optional[str]
    confidence: Optional[float]

class MessageWithObject(BaseModel):
    channel: Optional[str]
    message_id: Optional[int]
    message_text: Optional[str]
    message_date: Optional[datetime]
    object: Optional[str]
    confidence: Optional[float]

class ChannelActivityItem(BaseModel):
    day: str
    messages: int

class TopObjectItem(BaseModel):
    object: str
    mentions: int
