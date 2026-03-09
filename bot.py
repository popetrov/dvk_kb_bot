import os
import asyncio
import aiosqlite
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from openai import OpenAI

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")

ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "")

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN в .env")

if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY в .env")

if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID в .env")

allowed_ids = set()

if ALLOWED_USER_IDS:
    for x in ALLOWED_USER_IDS.split(","):
        x = x.strip()
        if x:
            allowed_ids.add(int(x))

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

client = OpenAI(api_key=OPENAI_API_KEY)

DB_PATH = "memory.db"

SYSTEM_INSTRUCTIONS = """
Ты — внутренний ассистент ДВК Финанс по бизнес-процессам.

Отвечай только на основании базы знаний.
Если точного ответа в базе знаний нет — так и скажи: "В базе знаний этого нет".

Учитывай контекст диалога.

Если пользователь задаёт короткий уточняющий вопрос:
"А если..."
"А кто..."
"А когда..."

трактуй его как продолжение предыдущего вопроса.

Формат ответа:

1. Коротко
2. Пошагово
3. Важно

Пиши простым рабочим языком.
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT,
            role TEXT,
            content TEXT
        )
        """)
        await db.commit()


async def save_message(chat_id, role, content):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO chat_history (chat_id, role, content) VALUES (?, ?, ?)",
            (chat_id, role, content),
        )
        await db.commit()


async def get_recent_messages(chat_id, limit=20):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            """
            SELECT role, content
            FROM chat_history
            WHERE chat_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (chat_id, limit),
        )

        rows = await cursor.fetchall()
        rows.reverse()

        return [{"role": r[0], "content": r[1]} for r in rows]


async def clear_chat_history(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM chat_history WHERE chat_id = ?",
            (chat_id,),
        )
        await db.commit()


@dp.message(Command("start"))
async def start_handler(message: types.Message):

    if allowed_ids and message.from_user.id not in allowed_ids:
        await message.answer(
            f"У вас нет доступа к базе знаний.\n\nВаш Telegram ID: {message.from_user.id}"
        )
        return

    await message.answer(
        "Привет. Я бот базы знаний ДВК Финанс.\n\nЗадай вопрос."
    )


@dp.message(Command("clear"))
async def clear_handler(message: types.Message):

    chat_id = str(message.chat.id)

    await clear_chat_history(chat_id)

    await message.answer("История этого чата очищена.")


@dp.message()
async def handle_question(message: types.Message):

    if allowed_ids and message.from_user.id not in allowed_ids:
        await message.answer("У вас нет доступа.")
        return

    question = (message.text or "").strip()

    if not question:
        await message.answer("Напиши вопрос текстом.")
        return

    chat_id = str(message.chat.id)

    await save_message(chat_id, "user", question)

    history = await get_recent_messages(chat_id, limit=20)

    input_data = [
        {"role": "system", "content": SYSTEM_INSTRUCTIONS}
    ]

    for item in history:
        input_data.append({
            "role": item["role"],
            "content": item["content"]
        })

    await message.answer("Понял. Думаю...")

    try:

        resp = client.responses.create(
            model="gpt-4.1",
            input=input_data,
            tools=[{
                "type": "file_search",
                "vector_store_ids": [VECTOR_STORE_ID],
                "max_num_results": 6
            }],
        )

        answer = resp.output_text

        if not answer or not answer.strip():
            answer = "Не смог сформировать ответ. Попробуй уточнить вопрос."

        await save_message(chat_id, "assistant", answer)

        await message.answer(answer)

    except Exception as e:

        text = str(e)

        if "insufficient_quota" in text or "Error code: 429" in text:

            await message.answer(
                "Сейчас AI временно недоступен: закончился лимит OpenAI API."
            )

        elif "Unauthorized" in text:

            await message.answer(
                "Ошибка авторизации Telegram. Проверь TELEGRAM_BOT_TOKEN."
            )

        else:

            await message.answer(
                f"Ошибка: {type(e).__name__}\n{text}"
            )


async def main():

    await init_db()

    await bot.delete_webhook(drop_pending_updates=True)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())