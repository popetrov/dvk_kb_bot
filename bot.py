import os
import asyncio
from dotenv import load_dotenv
from memory import init_db, save_message, get_recent_messages, clear_chat_history

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart
from aiogram.filters import Command

from openai import OpenAI

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "").strip()

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN в .env")
if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY в .env")
if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID в .env. Сначала запусти 01_create_vector_store.py")

allowed_ids = set()
if ALLOWED_USER_IDS:
    allowed_ids = {int(x.strip()) for x in ALLOWED_USER_IDS.split(",") if x.strip().isdigit()}

client = OpenAI(api_key=OPENAI_API_KEY)

SYSTEM_INSTRUCTIONS = """
Ты — внутренний ассистент ДВК Финанс по бизнес-процессам.

Отвечай только на основании базы знаний.
Если точного ответа в базе знаний нет — так и скажи: "В базе знаний этого нет".
Ничего не выдумывай.

Очень важно:
1. Если пользователь не указал свою роль, отвечай по умолчанию из позиции обычного сотрудника / менеджера / МОПа.
2. Не отвечай так, как будто вопрос задаёт руководитель отдела продаж, если это не сказано явно.
3. Не используй формулировки, где "ты" относится к РОПу, руководителю группы или другому руководителю, если пользователь явно не сказал, что он руководитель.
4. Если в базе знаний описан процесс для руководителя, а вопрос задан как от лица МОПа, объясняй этот процесс с точки зрения МОПа простым языком.
5. Если в вопросе есть местоимения "я", "мне", "у меня", трактуй их как позицию сотрудника, который задаёт вопрос, а не как автора документа.
6. Не копируй формулировки из документа буквально, если из-за этого меняется смысл роли.
7. Если есть неоднозначность, сначала кратко ответь в безопасной форме без неверного обращения по роли.

Формат ответа:
1. Коротко
2. Пошагово
3. Важно

Пиши простым, понятным, рабочим языком.
"""

dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: types.Message):
    await message.answer(
        "Привет! Я бот-база знаний ДВК (MVP).\n"
        "Задай вопрос по бизнес-процессу или мониторингу.\n"
        "Я отвечаю только по документам из базы знаний."
    )

@dp.message(Command("clear"))
async def clear_handler(message: types.Message):
    chat_id = str(message.chat.id)
    await clear_chat_history(chat_id)
    await message.answer("История этого чата очищена.")

@dp.message()
async def handle_question(message: types.Message):
    if allowed_ids and message.from_user and message.from_user.id not in allowed_ids:
        await message.answer("Доступ ограничен. Обратись к руководителю, чтобы тебя добавили.")
        return

    question = (message.text or "").strip()
    if not question:
        await message.answer("Напиши вопрос текстом.")
        return

    chat_id = str(message.chat.id)

    # Сохраняем вопрос пользователя
    await save_message(chat_id, "user", question)

    # Берём последние сообщения из этого чата
    history = await get_recent_messages(chat_id, limit=10)

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

        # Сохраняем ответ бота
        await save_message(chat_id, "assistant", answer)

        await message.answer(answer)

    except Exception as e:
        text = str(e)
        if "insufficient_quota" in text or "Error code: 429" in text:
            await message.answer(
                "Сейчас AI-ответы временно недоступны: закончился лимит OpenAI API."
            )
        else:
            await message.answer(f"Ошибка: {type(e).__name__}\n{e}")

async def main():
    await init_db()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())