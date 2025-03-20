import re
import aiofiles
import os
import csv
from typing import Optional
from datetime import datetime

import pandas as pd
from sqlalchemy import insert

from database import get_async_session
from db_operations.models import ParseSpimex
from config import setting


async def load_to_db() -> Optional[Exception]:
    """Загрука в БД"""
    if not os.path.exists(setting.TEMP_CSV) or (os.stat(setting.TEMP_CSV).st_size) == 0:
        print("CSV file empty")
        return None

    try:
        async for session in get_async_session():
            async with session.begin():
                async with aiofiles.open(setting.TEMP_CSV, "r", encoding="utf-8") as f:
                    contents = await f.read()

                reader = csv.reader(contents.splitlines(), delimiter="\t")

                batch_data = []
                for row in reader:
                    if len(row) < 10:
                        continue

                    batch_data.append({
                        "exchange_product_id": row[0],
                        "exchange_product_name": row[1],
                        "oil_id": row[6],
                        "delivery_basis_id": row[7],
                        "delivery_basis_name": row[2],
                        "delivery_type_id": row[8],
                        "volume": int(row[3]),
                        "total": int(row[4]),
                        "count": int(row[5]),
                        "date": datetime.strptime(row[9], "%Y-%m-%d").date(),
                    })

                if batch_data:
                    await session.execute(insert(ParseSpimex), batch_data)
                    print(f"✅ {len(batch_data)} записей добавлено в БД.")

        os.remove(setting.TEMP_CSV)

    except Exception as e:
        print(f"Mistake for load db: {e}")


async def process_file(filepath: str) -> Optional[Exception]:
    """Читает Excel-файл, парсит данные и сохраняет в CSV."""
    try:
        engine = "openpyxl" if filepath.endswith(".xlsx") else "xlrd"

        df = pd.read_excel(filepath, sheet_name="TRADE_SUMMARY", engine=engine)
        df.dropna(how="all", inplace=True)

        header_index = None
        for idx, row in df.iterrows():
            row_clean = row.astype(str).str.replace(r"[\n\t\xa0]", " ", regex=True).str.strip()
            if ("Код Инструмента" in row_clean.values) and ("Наименование Инструмента" in row_clean.values):
                header_index = idx
                break

        if header_index is None:
            print(f"Headers not exists")
            return

        df.columns = df.iloc[header_index]
        df = df.iloc[header_index + 1:].reset_index(drop=True)
        df.columns = df.columns.astype(str).str.replace(
            r"[\n\t\xa0]", " ", regex=True).str.strip().str.lower()

        columns_map = {
            "код инструмента": "exchange_product_id",
            "наименование инструмента": "exchange_product_name",
            "базис поставки": "delivery_basis_name",
            "объем договоров в единицах измерения": "volume",
            "обьем договоров, руб.": "total",
            "количество договоров, шт.": "count"
        }

        df = df[list(columns_map.keys())].copy()
        df.rename(columns=columns_map, inplace=True)

        df = df[~df["exchange_product_id"].astype(str).str.contains(
            r"^Итого", case=False, na=False)]
        df = df[df["exchange_product_id"].astype(str).str.strip() != ""]
        df = df[df["count"].astype(str) != "-"]

        df[["volume", "total", "count"]] = df[["volume", "total", "count"]].replace("-", "0")
        df[["volume", "total", "count"]] = df[["volume", "total", "count"]].apply(
            pd.to_numeric, errors="coerce"
        ).fillna(0).astype(int)

        df["oil_id"] = df["exchange_product_id"].str[:4]
        df["delivery_basis_id"] = df["exchange_product_id"].str[4:7]
        df["delivery_type_id"] = df["exchange_product_id"].str[-1]

        filename = os.path.basename(filepath)

        date_match = re.search(r"(\d{8})", filename)
        if date_match:
            date_str = date_match.group()  # Берём найденное совпадение
            df["date"] = pd.to_datetime(date_str, format="%Y%m%d")
        else:
            raise ValueError(f"Date not found in {filename}")

        df = df.astype(str).apply(lambda col: col.map(lambda x: x.replace("\t", " ").strip()))

        df.replace("nan", "", inplace=True)
        df = df[df["exchange_product_id"].str.strip() != ""]

        async with aiofiles.open(setting.TEMP_CSV, mode="a", encoding="utf-8") as f:
            await f.write(df.to_csv(sep="\t", index=False, header=False))

        print(f"Data from {filepath} add in {setting.TEMP_CSV}")

    except Exception as e:
        print(f"Mistake proccessing {filepath}: {e}")

    os.remove(filepath)