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
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN")

if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY")

if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID")

allowed_ids = set()

if ALLOWED_USER_IDS:
    for x in ALLOWED_USER_IDS.split(","):
        x = x.strip()
        if x.isdigit():
            allowed_ids.add(int(x))

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
client = OpenAI(api_key=OPENAI_API_KEY)

DB_PATH = "memory.db"

SYSTEM_INSTRUCTIONS = """
Ты — внутренний ассистент ДВК Финанс по бизнес-процессам.

Отвечай только на основании базы знаний.
Если точного ответа в базе знаний нет — так и скажи: "В базе знаний этого нет".
Ничего не выдумывай.

Очень важно:
1. Если пользователь не указал свою роль, отвечай по умолчанию из позиции обычного сотрудника / менеджера / МОПа.
2. Если пользователь пишет "я МОП", "я менеджер", "я сотрудник", это часть вопроса. Нужно сразу ответить по существу именно для этой роли.
3. Если пользователь пишет "я РОП" или "я руководитель", отвечай по существу из роли руководителя.
4. Если новый вопрос короткий и похож на уточнение к предыдущему, обязательно учитывай предыдущий вопрос и предыдущий ответ в этом чате.
5. Вопросы вида "а если...", "а кто...", "а когда...", "а если клиент новый?" нужно трактовать как продолжение предыдущего вопроса.
6. Не отвечай фразами вроде "поняла, буду отвечать как МОП". Сразу давай содержательный ответ.
7. Обращайся к пользователю по имени, если оно передано в сообщении системы.
8. Не используй излишне официальный стиль.

Формат ответа:
1. Коротко
2. Пошагово
3. Важно

Пиши простым, понятным, рабочим языком.
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
    return [{"role": row[0], "content": row[1]} for row in rows]


async def clear_chat_history(chat_id):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM chat_history WHERE chat_id = ?",
            (chat_id,),
        )
        await db.commit()


def get_display_name(user: types.User) -> str:
    if user.first_name and user.first_name.strip():
        return user.first_name.strip()

    if user.username and user.username.strip():
        return f"@{user.username.strip()}"

    return "котик-продаван"


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    if allowed_ids and message.from_user.id not in allowed_ids:
        await message.answer(
            f"У вас нет доступа к базе знаний.\n\nВаш Telegram ID: {message.from_user.id}"
        )
        return

    display_name = get_display_name(message.from_user)

    await message.answer(
        f"Привет, {display_name}.\n\n"
        f"Я бот базы знаний ДВК Финанс.\n"
        f"Задай вопрос по бизнес-процессам, мониторингу или регламентам.\n\n"
        f"Если хочешь начать диалог заново, отправь /clear"
    )


@dp.message(Command("clear"))
async def clear_handler(message: types.Message):
    if allowed_ids and message.from_user.id not in allowed_ids:
        await message.answer("У вас нет доступа.")
        return

    chat_id = str(message.chat.id)
    display_name = get_display_name(message.from_user)

    await clear_chat_history(chat_id)
    await message.answer(f"{display_name}, история этого чата очищена.")


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
    display_name = get_display_name(message.from_user)

    await save_message(chat_id, "user", question)

    history = await get_recent_messages(chat_id, limit=20)

    input_data = [
        {"role": "system", "content": SYSTEM_INSTRUCTIONS},
        {
            "role": "system",
            "content": f"Пользователя зовут: {display_name}. Обращайся к нему по этому имени естественно и без перебора."
        },
        {
            "role": "system",
            "content": "Если последнее сообщение пользователя короткое и начинается с 'а', 'а если', 'а кто', 'а когда', трактуй его как продолжение предыдущего вопроса в этом чате."
        }
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
                "Ошибка авторизации Telegram. Нужно проверить TELEGRAM_BOT_TOKEN."
            )
        else:
            await message.answer(f"Ошибка: {type(e).__name__}\n{text}")


async def main():
    await init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())