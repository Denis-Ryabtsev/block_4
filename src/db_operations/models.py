import datetime

from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import BigInteger, Date, func

from database import Base


class ParseSpimex(Base):
    __tablename__ = 'spimex_trading_results'

    id: Mapped[int] = mapped_column(
        primary_key=True, autoincrement=True
    )
    exchange_product_id: Mapped[str]
    exchange_product_name: Mapped[str]
    oil_id: Mapped[str]
    delivery_basis_id: Mapped[str]
    delivery_basis_name: Mapped[str]
    delivery_type_id: Mapped[str]
    volume: Mapped[int]
    total: Mapped[int] = mapped_column(BigInteger)
    count: Mapped[int]
    date: Mapped[datetime.date] = mapped_column(
        Date, nullable=False
    )
    created_on: Mapped[datetime.date] = mapped_column(
        Date, server_default=func.current_date()
    )
    updated_on: Mapped[datetime.date] = mapped_column(
        Date, server_default=func.current_date(), onupdate=func.current_date()
    )