import asyncio
import aiohttp
import os

from network_operations.download_files import download_files, get_links
from db_operations.parse_files import load_to_db, process_file
from config import setting


async def main():
    semaphore = asyncio.Semaphore(setting.SEM_LIMIT)
    async with aiohttp.ClientSession() as session:
        links = await get_links(session, setting.URL)
        if not links:
            print("Links not found")
            return

        print(f"Find {len(links)} links for download")

        download_tasks = [
            asyncio.create_task(
                download_files(session, semaphore, link)) for link in links
        ]
        await asyncio.gather(*download_tasks)

        downloaded_files = [
            f for f in os.listdir(setting.DOWNLOAD_FOLDER) \
                if f.endswith(".xls") or f.endswith(".xlsx")
        ]
        filepaths = [os.path.join(setting.DOWNLOAD_FOLDER, f) for f in downloaded_files]

        if not filepaths:
            print("Files not exists")
            return

        await asyncio.gather(*(process_file(fp) for fp in filepaths))

        await load_to_db()

        print("Success load to db")


if __name__ == '__main__':
    asyncio.run(main())
