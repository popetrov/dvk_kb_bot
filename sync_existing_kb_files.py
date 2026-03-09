import os
import asyncio
import aiosqlite
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")
DB_PATH = os.getenv("DB_PATH", "bot_data.db")

if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY")

if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID")

client = OpenAI(api_key=OPENAI_API_KEY)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS kb_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            openai_file_id TEXT NOT NULL
        )
        """)
        await db.commit()


async def save_kb_file_id(filename: str, openai_file_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO kb_files (filename, openai_file_id)
            VALUES (?, ?)
            ON CONFLICT(filename) DO UPDATE SET openai_file_id = excluded.openai_file_id
            """,
            (filename, openai_file_id)
        )
        await db.commit()


async def main():
    await init_db()

    print("Получаю список файлов из vector store...")

    files_page = client.vector_stores.files.list(
        vector_store_id=VECTOR_STORE_ID
    )

    count = 0

    for item in files_page.data:
        file_id = item.id

        file_obj = client.files.retrieve(file_id)
        filename = getattr(file_obj, "filename", None)

        if not filename:
            print(f"Пропущен file_id={file_id}, не удалось получить имя файла.")
            continue

        await save_kb_file_id(filename, file_id)
        print(f"Добавлен в локальную базу: {filename} -> {file_id}")
        count += 1

    print(f"\nСинхронизация завершена. Добавлено файлов: {count}")


if __name__ == "__main__":
    asyncio.run(main())