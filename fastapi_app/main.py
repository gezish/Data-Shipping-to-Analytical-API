# fastapi_app/main.py
import os
from fastapi import FastAPI, HTTPException, Query, Depends
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
import asyncio

from . import db
from .schemas import MessageRow, DetectionRow, MessageWithObject, ChannelActivityItem, TopObjectItem

ANALYTICS_SCHEMA = os.getenv("ANALYTICS_SCHEMA", "analytics")  # default schema where dbt created models

app = FastAPI(title="Telegram Analytical API", version="1.0")

# Allow CORS in development; adjust origins in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    await db.init_db_pool()

@app.on_event("shutdown")
async def shutdown():
    await db.close_db_pool()

# Dependency helper to get a connection from the pool
async def get_conn():
    pool = await db.init_db_pool()
    async with pool.acquire() as conn:
        yield conn

# -------------------------
# 1) Search messages (keyword)
# -------------------------
@app.get("/api/search", response_model=List[MessageRow])
async def search_messages(q: str = Query(..., min_length=2), limit: int = Query(50, ge=1, le=1000), conn=Depends(get_conn)):
    """
    Search messages text for a keyword (case-insensitive).
    """
    sql = f"""
        select channel, message_id, message_text, message_date, views, has_media
        from {ANALYTICS_SCHEMA}.fct_messages
        where message_text ilike $1
        order by message_date desc
        limit $2
    """
    pattern = f"%{q}%"
    try:
        rows = await conn.fetch(sql, pattern, limit)
        return [MessageRow(**dict(r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# 2) Detections / YOLO results
# -------------------------
@app.get("/api/detections", response_model=List[DetectionRow])
async def get_detections(channel: Optional[str] = None, object_name: Optional[str] = None,
                         min_confidence: float = 0.0, limit: int = 100, conn=Depends(get_conn)):
    """
    Return YOLO detections with optional filters.
    """
    where_clauses = []
    params = []
    idx = 1
    if channel:
        where_clauses.append(f"channel = ${idx}"); params.append(channel); idx += 1
    if object_name:
        where_clauses.append(f"object ilike ${idx}"); params.append(f"%{object_name}%"); idx += 1
    if min_confidence and min_confidence > 0:
        where_clauses.append(f"confidence >= ${idx}"); params.append(min_confidence); idx += 1

    where_sql = (" where " + " and ".join(where_clauses)) if where_clauses else ""
    sql = f"""
        select channel, message_id, image_path, object, confidence
        from {ANALYTICS_SCHEMA}.fct_image_detections
        {where_sql}
        order by confidence desc
        limit ${idx}
    """
    params.append(limit)
    try:
        rows = await conn.fetch(sql, *params)
        return [DetectionRow(**dict(r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# 3) Channel activity
# -------------------------
@app.get("/api/channel-activity/{channel}", response_model=List[ChannelActivityItem])
async def channel_activity(channel: str, days: int = Query(90, ge=1, le=365), conn=Depends(get_conn)):
    """
    Returns daily message counts for the channel for the last `days` days.
    """
    sql = f"""
        select to_char(day, 'YYYY-MM-DD') as day, cnt from (
            select date_trunc('day', message_date) as day, count(*) as cnt
            from {ANALYTICS_SCHEMA}.fct_messages
            where channel = $1 and message_date >= now() - ($2 * interval '1 day')
            group by day
            order by day
        ) t;
    """
    try:
        rows = await conn.fetch(sql, channel, days)
        return [ChannelActivityItem(day=r["day"], messages=r["cnt"]) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# 4) Top objects detected (aggregated)
# -------------------------
@app.get("/api/top-objects", response_model=List[TopObjectItem])
async def top_objects(limit: int = Query(20, ge=1, le=200), conn=Depends(get_conn)):
    """
    Returns most frequently detected objects across images.
    """
    sql = f"""
        select object, count(*) as mentions
        from {ANALYTICS_SCHEMA}.fct_image_detections
        group by object
        order by mentions desc
        limit $1
    """
    try:
        rows = await conn.fetch(sql, limit)
        return [TopObjectItem(object=r["object"], mentions=r["mentions"]) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------
# 5) Messages with objects joined query (paginated)
# -------------------------
@app.get("/api/messages-with-objects", response_model=List[MessageWithObject])
async def messages_with_objects(channel: Optional[str] = None,
                                object_name: Optional[str] = None,
                                limit: int = Query(50, ge=1, le=1000),
                                offset: int = Query(0, ge=0),
                                conn=Depends(get_conn)):
    """
    Return messages joined to detected objects (if any). Pagination via limit/offset.
    """
    where_clauses = []
    params = []
    idx = 1
    if channel:
        where_clauses.append(f"m.channel = ${idx}"); params.append(channel); idx += 1
    if object_name:
        where_clauses.append(f"d.object ilike ${idx}"); params.append(f"%{object_name}%"); idx += 1

    where_sql = (" where " + " and ".join(where_clauses)) if where_clauses else ""
    sql = f"""
        select m.channel, m.message_id, m.message_text, m.message_date, d.object, d.confidence
        from {ANALYTICS_SCHEMA}.messages_with_objects m
        left join {ANALYTICS_SCHEMA}.fct_image_detections d
          on m.channel = d.channel and m.message_id = d.message_id
        {where_sql}
        order by m.message_date desc
        limit ${idx} offset ${idx + 1}
    """
    params.extend([limit, offset])
    try:
        rows = await conn.fetch(sql, *params)
        return [MessageWithObject(**dict(r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
