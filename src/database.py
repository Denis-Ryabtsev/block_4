import asyncio
from typing import AsyncGenerator, Optional

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker
)

from config import setting


class Base(DeclarativeBase):
    pass


async_engine = create_async_engine(
    url=setting.DB_URL, echo=True, pool_size=5, max_overflow=10
)

async_session = async_sessionmaker(async_engine)


async def get_async_session() -> Optional[AsyncGenerator]:
    async with async_session() as session:
        await asyncio.sleep(0)
        yield session