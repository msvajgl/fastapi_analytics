import os
from typing import List
from ..db.config import DATABASE_URL
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session, select

from timescaledb.hyperfunctions import time_bucket
from pprint import pprint
from sqlalchemy import func, case
from datetime import datetime, timedelta, timezone
from sqlmodel import Session, select

from api.db.session import get_session
from .models import (
    EventCreateSchema,
    EventModel, 
    EventListSchema,
    # EventUpdateSchema,
    get_utc_now,
    EventBucketSchema
    )

router = APIRouter()

DEFAULT_LOOKUP_PAGES = ['/','/about', '/contact', '/pages', '/pricing',
        '/blog', '/products', '/login', '/signup', '/dashboard',
        '/settings']

# GET /api/events
@router.get("/", response_model=List[EventBucketSchema])
def read_events(
    duration: str = Query(default="1 day"),
    pages: List = Query(default=None),
    sessions: Session = Depends(get_session)
    ):
    # a bunch of items in a table
    os_case = case(
        (EventModel.user_agent.ilike('%windows%'), 'Windows'),
        (EventModel.user_agent.ilike('%macintosh%'), 'MacOS'),
        (EventModel.user_agent.ilike('%iphone%'), 'iOS'),
        (EventModel.user_agent.ilike('%android%'), 'Android'),
        (EventModel.user_agent.ilike('%linux%'), 'Linux'),
        else_='Other'
    ).label('operating_system')

    bucket = time_bucket("1 day", EventModel.time)
    lookup_pages = pages if isinstance(pages, list) and len(pages)>0 else DEFAULT_LOOKUP_PAGES
   
    query = (
        select(
            bucket.label('bucket'),
            os_case,
            func.avg(EventModel.duration).label("avg_duration"),
            EventModel.page.label('page'),
            func.count().label('count'))
        .where(
            EventModel.page.in_(lookup_pages)
        )
        .group_by(
            bucket,
            os_case,
            EventModel.page
        )
        .order_by(
            bucket,
            os_case,
            EventModel.page
        )
    )
    results = sessions.exec(query).fetchall()
    return results

# POST /api/events
@router.post("/", response_model=EventModel)
def create_event(
        payload:EventCreateSchema, 
        session: Session = Depends(get_session)):
    # a bunch of items in a table
    data = payload.model_dump() # payload -> dict -> pydantic
    obj = EventModel.model_validate(data)
    session.add(obj)
    session.commit()
    session.refresh(obj)
    return obj

# GET /api/events/{2}
@router.get("/{event_id}", response_model=EventModel)
def get_event(event_id:int, sessions: Session = Depends(get_session)):
    query = select(EventModel).where(EventModel.id == event_id)
    results = sessions.exec(query).first()
    if not results:
        raise HTTPException(status_code=404, detail="Event not found")
    return results

# PUT /api/events/{2}
# @router.put("/{event_id}", response_model=EventModel)
# def update_event(
#     event_id:int, 
#     payload:EventUpdateSchema, 
#     sessions: Session = Depends(get_session)):

#     query = select(EventModel).where(EventModel.id == event_id)
#     obj = sessions.exec(query).first()
#     if not obj:
#         raise HTTPException(status_code=404, detail="Event not found")
#     data = payload.model_dump() # payload -> dict ->pydantic
#     for k, v in data.items():
#         setattr(obj, k, v)

#     obj.updated_at = get_utc_now()
#     print("------------")
#     print(obj)

#     sessions.add(obj)
#     sessions.commit()
#     sessions.refresh(obj)
#     return obj