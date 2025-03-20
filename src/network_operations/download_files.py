import asyncio
from pathlib import Path
from urllib.parse import urljoin
from datetime import datetime

import aiofiles
from bs4 import BeautifulSoup

from config import setting


Path(setting.DOWNLOAD_FOLDER).mkdir(parents=True, exist_ok=True)
csv_path = Path(setting.TEMP_CSV)
if not csv_path.exists():
    csv_path.touch()
    
CURRENT_DATE=datetime(2025, 1, 1)

async def get_page(session, url):
    async with session.get(url) as response:
        return await response.text()


async def get_links(session, start_url):
    links = []
    pending_pages = [start_url]
    visited_pages = set()

    while pending_pages:
        url = pending_pages.pop(0)
        if url in visited_pages:
            continue
        visited_pages.add(url)

        html = await get_page(session, url)
        soup = await asyncio.to_thread(lambda: BeautifulSoup(html, 'html.parser'))

        page_has_fresh_files = False

        for item in soup.select(".accordeon-inner__wrap-item"):
            a_tag = item.select_one("a.accordeon-inner__item-title.link.xls")
            if not a_tag or "Бюллетень" not in a_tag.get_text():
                continue

            href = urljoin(setting.FILE_URL, a_tag["href"])

            date_tag = item.select_one(".accordeon-inner__item-inner__title span")
            if not date_tag:
                continue

            trade_date = datetime.strptime(date_tag.get_text(), "%d.%m.%Y")

            if trade_date >= CURRENT_DATE:
                links.append(href)
                page_has_fresh_files = True

        if not page_has_fresh_files:
            break

        for a in soup.select(".bx-pagination-container ul li a"):
            page_url = urljoin(setting.FILE_URL, a["href"])
            if page_url not in visited_pages:
                pending_pages.append(page_url)

    return links


async def download_files(session, semaphore, url):
    async with semaphore:
        filename = url.split('/')[-1].split('?')[0]
        filepath = Path(setting.DOWNLOAD_FOLDER) / filename

        try:
            async with session.get(url) as response:
                if response.status == 200:
                    async with aiofiles.open(filepath, 'wb') as f:
                        await f.write(await response.read())
                else:
                    print(f'Mistake load {filename}')
        except Exception as e:
            print(f'Mistake install {filename}: {e}')
