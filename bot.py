import os
import asyncio
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types
from aiogram.filters import CommandStart, Command

from openai import OpenAI

from memory import init_db, save_message, get_recent_messages, clear_chat_history

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "").strip()

if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("Нет TELEGRAM_BOT_TOKEN")
if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY")
if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID")

allowed_ids = set()
if ALLOWED_USER_IDS:
    allowed_ids = {
        int(x.strip())
        for x in ALLOWED_USER_IDS.split(",")
        if x.strip().isdigit()
    }

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
8. Если пользователь прямо пишет "я МОП", "я менеджер", "я сотрудник", отвечай именно из этой роли.
9. Если пользователь пишет "я РОП" или "я руководитель", отвечай из роли руководителя.

Формат ответа:
1. Коротко
2. Пошагово
3. Важно

Пиши простым, понятным, рабочим языком.
"""

dp = Dispatcher()


@dp.message(CommandStart())
async def start_handler(message: types.Message):
    if allowed_ids and message.from_user and message.from_user.id not in allowed_ids:
        await message.answer("Доступ ограничен. Обратись к руководителю, чтобы тебя добавили.")
        return

    await message.answer(
        "Привет! Я внутренний бот базы знаний ДВК Финанс.\n\n"
        "Я могу отвечать на вопросы по бизнес-процессам, мониторингу и другим материалам из базы знаний.\n\n"
        "Если хочешь начать диалог заново, отправь команду /clear"
    )


@dp.message(Command("clear"))
async def clear_handler(message: types.Message):
    if allowed_ids and message.from_user and message.from_user.id not in allowed_ids:
        await message.answer("Доступ ограничен. Обратись к руководителю, чтобы тебя добавили.")
        return

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

    # Сохраняем вопрос пользователя в память
    await save_message(chat_id, "user", question)

    # Достаём последние сообщения из памяти этого чата
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

        # Сохраняем ответ бота в память
        await save_message(chat_id, "assistant", answer)

        await message.answer(answer)

    except Exception as e:
        text = str(e)

        if "insufficient_quota" in text or "Error code: 429" in text:
            await message.answer(
                "Сейчас AI-ответы временно недоступны: закончился лимит OpenAI API."
            )
        elif "Unauthorized" in text:
            await message.answer(
                "Ошибка авторизации Telegram. Нужно проверить TELEGRAM_BOT_TOKEN."
            )
        else:
            await message.answer(f"Ошибка: {type(e).__name__}\n{e}")


async def main():
    await init_db()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())