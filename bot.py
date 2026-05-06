import os
import asyncio
import tempfile
from pathlib import Path

import aiosqlite
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from openai import OpenAI

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")
ALLOWED_USER_IDS = os.getenv("ALLOWED_USER_IDS", "")
ADMIN_USER_IDS = os.getenv("ADMIN_USER_IDS", "")
DB_PATH = os.getenv("DB_PATH", "bot_data.db")

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

admin_ids = set()
if ADMIN_USER_IDS:
    for x in ADMIN_USER_IDS.split(","):
        x = x.strip()
        if x.isdigit():
            admin_ids.add(int(x))

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()
client = OpenAI(api_key=OPENAI_API_KEY)

SUPPORTED_EXTENSIONS = {
    ".doc", ".docx", ".pdf", ".txt", ".md", ".markdown",
    ".xls", ".xlsx", ".csv", ".rtf", ".xml", ".json",
    ".ppt", ".pptx", ".odt", ".html", ".htm", ".zip"
}

waiting_for_kb_file = set()
waiting_for_kb_delete = set()

SYSTEM_INSTRUCTIONS = """
Ты — внутренний ассистент ДВК Финанс по бизнес-процессам.

Отвечай строго только на основании базы знаний и контекста текущего диалога.
Если в базе знаний нет точного ответа, так и пиши: "В базе знаний этого нет".

Правила:
1. Не выдумывай.
2. Если вопрос короткий и уточняющий, обязательно связывай его с предыдущими сообщениями в этом чате.
3. Если пользователь пишет "я МОП", "я менеджер", "я сотрудник" — отвечай именно для этой роли.
4. Если пользователь не указал роль, отвечай по умолчанию из позиции сотрудника / МОПа.
5. Если в базе знаний есть ответ, но он описан с позиции руководителя, объясняй его простым языком с позиции сотрудника.
6. Если есть несколько вариантов трактовки, выбирай тот, который лучше всего соответствует последним сообщениям диалога.
7. Не отвечай общими фразами вроде "буду учитывать это дальше". Сразу отвечай по существу.
8. Если вопрос связан с предыдущим, не пиши "В базе знаний этого нет", пока не попробуешь связать его с предыдущим вопросом.
9. Если ответа нет, сначала коротко напиши, что именно не найдено.
10. Обращайся к пользователю естественно, по имени.

Формат ответа:
1. Коротко
2. Пошагово
3. Важно

Пиши простым рабочим языком.
"""


def ensure_db_path():
    db_file = Path(DB_PATH)
    if db_file.parent and str(db_file.parent) not in (".", ""):
        db_file.parent.mkdir(parents=True, exist_ok=True)


async def init_db():
    ensure_db_path()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS chat_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT,
            role TEXT,
            content TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS kb_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            openai_file_id TEXT NOT NULL
        )
        """)
        await db.commit()


async def save_message(chat_id: str, role: str, content: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO chat_history (chat_id, role, content) VALUES (?, ?, ?)",
            (chat_id, role, content),
        )
        await db.commit()


async def get_recent_messages(chat_id: str, limit: int = 30):
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


async def clear_chat_history(chat_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM chat_history WHERE chat_id = ?",
            (chat_id,),
        )
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


async def get_all_kb_files():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT filename FROM kb_files ORDER BY filename"
        )
        rows = await cursor.fetchall()

    return [r[0] for r in rows]


async def delete_kb_file(filename: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM kb_files WHERE filename = ?",
            (filename,)
        )
        await db.commit()


def normalize_filename(name) -> str:
    if name is None:
        return ""

    if not isinstance(name, str):
        name = str(name)

    return name.strip().lower()


def list_vector_store_files():
    result = []

    page = client.vector_stores.files.list(
        vector_store_id=VECTOR_STORE_ID,
        limit=100
    )

    while True:
        for item in page.data:
            try:
                file_obj = client.files.retrieve(item.id)
                filename = getattr(file_obj, "filename", None) or item.id

                result.append({
                    "filename": filename,
                    "file_id": item.id
                })

            except Exception as e:
                print(f"Не удалось получить файл из Vector Store: {type(e).__name__}: {e}", flush=True)

        if not getattr(page, "has_more", False):
            break

        page = client.vector_stores.files.list(
            vector_store_id=VECTOR_STORE_ID,
            limit=100,
            after=page.data[-1].id
        )

    return result


def delete_all_vector_store_files_by_filename(filename: str) -> int:
    target = normalize_filename(filename)
    deleted_count = 0

    for item in list_vector_store_files():
        if normalize_filename(item["filename"]) == target:
            try:
                client.vector_stores.files.delete(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=item["file_id"]
                )
                deleted_count += 1
            except Exception as e:
                print(f"Не удалось удалить файл {item['file_id']}: {type(e).__name__}: {e}", flush=True)

    return deleted_count


def get_display_name(user: types.User) -> str:
    if user.first_name and user.first_name.strip():
        return user.first_name.strip()

    if user.username and user.username.strip():
        return f"@{user.username.strip()}"

    return "котик-продаван"


def is_allowed(user_id: int) -> bool:
    if not allowed_ids:
        return True
    return user_id in allowed_ids


def is_admin(user_id: int) -> bool:
    return user_id in admin_ids


@dp.message(Command("start"))
async def start_handler(message: types.Message):
    print("START COMMAND RECEIVED", message.from_user.id, message.chat.id, flush=True)

    if not is_allowed(message.from_user.id):
        await message.answer(
            f"У вас нет доступа к базе знаний.\n\nВаш Telegram ID: {message.from_user.id}"
        )
        return

    display_name = get_display_name(message.from_user)

    await message.answer(
        f"Привет, {display_name}.\n\n"
        f"Я бот базы знаний ДВК Финанс.\n"
        f"Задай вопрос по бизнес-процессам, мониторингу или регламентам.\n\n"
        f"Команды:\n"
        f"/clear — очистить историю диалога\n"
        f"/whoami — показать твои данные Telegram\n"
        f"/update_kb — обновить базу знаний (только для админа)\n"
        f"/kb_delete — удалить файл из базы знаний (только для админа)"
    )


@dp.message(Command("clear"))
async def clear_handler(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("У вас нет доступа.")
        return

    chat_id = str(message.chat.id)
    display_name = get_display_name(message.from_user)

    await clear_chat_history(chat_id)
    await message.answer(f"{display_name}, история этого чата очищена.")


@dp.message(Command("whoami"))
async def whoami_handler(message: types.Message):
    user = message.from_user
    first_name = user.first_name or "нет"
    last_name = user.last_name or "нет"
    username = f"@{user.username}" if user.username else "нет"

    text = (
        "Твоя информация:\n\n"
        f"Имя: {first_name}\n"
        f"Фамилия: {last_name}\n"
        f"Username: {username}\n\n"
        f"Telegram ID: {user.id}\n"
        f"Chat ID: {message.chat.id}"
    )

    await message.answer(text)


@dp.message(Command("update_kb"))
async def update_kb_handler(message: types.Message):
    print("UPDATE_KB COMMAND RECEIVED", message.from_user.id, message.chat.id, flush=True)

    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только администратору.")
        return

    chat_id = str(message.chat.id)
    waiting_for_kb_file.add(chat_id)
    waiting_for_kb_delete.discard(chat_id)

    await message.answer(
        "Пришли файл документом для обновления базы знаний.\n\n"
        "Поддерживаются форматы: doc, docx, pdf, txt, md, xls, xlsx, csv, ppt, pptx, odt, html, json, xml, zip."
    )


@dp.message(Command("kb_delete"))
async def kb_delete_command(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только администраторам.")
        return

    await message.answer("Получаю актуальный список файлов из OpenAI Vector Store...")

    vector_files = list_vector_store_files()
    files = sorted([item["filename"] for item in vector_files])

    if not files:
        await message.answer("В OpenAI Vector Store сейчас нет файлов.")
        return

    chat_id = str(message.chat.id)
    waiting_for_kb_delete.add(chat_id)
    waiting_for_kb_file.discard(chat_id)

    header = "Актуальные файлы в базе знаний:\n\n"
    current_text = header
    messages_to_send = []

    for f in files:
        line = f"• {f}\n"

        if len(current_text) + len(line) > 3500:
            messages_to_send.append(current_text)
            current_text = line
        else:
            current_text += line

    if current_text:
        messages_to_send.append(current_text)

    for part in messages_to_send:
        await message.answer(part)

    await message.answer("Отправь точное имя файла, который нужно удалить.")

@dp.message(Command("kb_deduplicate"))
async def kb_deduplicate_command(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только администраторам.")
        return

    await message.answer("Начинаю удаление дублей в OpenAI Vector Store...")

    try:
        vector_files = list_vector_store_files()

        grouped = {}
        for item in vector_files:
            filename_key = normalize_filename(item["filename"])
            grouped.setdefault(filename_key, []).append(item)

        deleted_count = 0
        duplicate_groups = 0

        for filename_key, items in grouped.items():
            if len(items) <= 1:
                continue

            duplicate_groups += 1

            # Оставляем первый файл, остальные удаляем
            files_to_delete = items[1:]

            for file_item in files_to_delete:
                try:
                    client.vector_stores.files.delete(
                        vector_store_id=VECTOR_STORE_ID,
                        file_id=file_item["file_id"]
                    )
                    deleted_count += 1
                except Exception as e:
                    print(
                        f"Ошибка удаления дубля {file_item['filename']} {file_item['file_id']}: "
                        f"{type(e).__name__}: {e}",
                        flush=True
                    )

        await clear_chat_history(str(message.chat.id))

        await message.answer(
            f"Готово.\n\n"
            f"Групп файлов с дублями: {duplicate_groups}\n"
            f"Удалено дублей: {deleted_count}\n\n"
            f"По каждому имени файла оставлен один экземпляр."
        )

    except Exception as e:
        await message.answer(
            f"Не удалось удалить дубли.\n\n"
            f"Ошибка: {type(e).__name__}\n{e}"
        )

@dp.message(Command("kb_clear_all"))
async def kb_clear_all(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("Только для админа")
        return

    await message.answer("Начинаю ПОЛНУЮ очистку Vector Store...")

    deleted = 0

    try:
        for item in client.vector_stores.files.list(
            vector_store_id=VECTOR_STORE_ID,
            limit=100
        ):
            try:
                client.vector_stores.files.delete(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=item.id
                )
                deleted += 1
            except Exception as e:
                print(f"ERROR DELETE: {e}", flush=True)

        # чистим локальную базу
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM kb_files")
            await db.commit()

        await message.answer(
            f"Готово.\n\nУдалено файлов: {deleted}"
        )

    except Exception as e:
        await message.answer(
            f"Ошибка очистки:\n{type(e).__name__}\n{e}"
        )

@dp.message(Command("kb_status"))
async def kb_status_command(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer("Эта команда доступна только администраторам.")
        return

    await message.answer("Проверяю реальное содержимое OpenAI Vector Store...")

    try:
        files = list_vector_store_files()
        files_sorted = sorted(files, key=lambda x: x["filename"].lower())

        await message.answer(f"Всего файлов в Vector Store: {len(files_sorted)}")

        if not files_sorted:
            await message.answer("База знаний пустая.")
            return

        current_text = "Файлы в базе знаний:\n\n"

        for item in files_sorted:
            line = f"• {item['filename']}\n"

            if len(current_text) + len(line) > 3500:
                await message.answer(current_text)
                current_text = line
            else:
                current_text += line

        if current_text:
            await message.answer(current_text)

    except Exception as e:
        await message.answer(
            f"Не удалось проверить базу знаний.\n\n"
            f"Ошибка: {type(e).__name__}\n{e}"
        )

@dp.message(F.document)
async def document_handler(message: types.Message):
    chat_id = str(message.chat.id)

    print("DOCUMENT HANDLER STARTED", flush=True)
    print("chat_id:", chat_id, flush=True)
    print("waiting_for_kb_file:", waiting_for_kb_file, flush=True)

    if chat_id not in waiting_for_kb_file:
        print("DOCUMENT IGNORED: chat_id not waiting", flush=True)
        return

    if not is_admin(message.from_user.id):
        waiting_for_kb_file.discard(chat_id)
        await message.answer("Эта команда доступна только администратору.")
        return

    document = message.document
    filename = document.file_name or "document"
    ext = os.path.splitext(filename)[1].lower()
    display_name = get_display_name(message.from_user)

    if ext not in SUPPORTED_EXTENSIONS:
        waiting_for_kb_file.discard(chat_id)
        await message.answer(
            f"{display_name}, файл получен, но формат {ext or 'без расширения'} не поддерживается."
        )
        return

    await message.answer(
        f"{display_name}, файл «{filename}» получен.\n"
        f"Начинаю обновление базы знаний..."
    )

    temp_path = None

    try:
        await message.answer("Шаг 0/4: скачиваю файл из Telegram...")

        tg_file = await bot.get_file(document.file_id)

        safe_filename = os.path.basename(filename)
        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, safe_filename)

        await bot.download_file(tg_file.file_path, destination=temp_path)
        await message.answer("Шаг 1/4: удаляю старые версии файла из OpenAI Vector Store...")
        deleted_count = 0

        await message.answer("Шаг 2/4: очищаю локальную запись о файле...")
        await delete_kb_file(filename)

        await message.answer("Шаг 3/4: загружаю новый файл в OpenAI...")
        with open(temp_path, "rb") as f:
            uploaded = client.files.create(
                file=f,
                purpose="assistants"
            )

        await message.answer("Шаг 4/4: добавляю новый файл в базу знаний...")
        client.vector_stores.files.create(
            vector_store_id=VECTOR_STORE_ID,
            file_id=uploaded.id
        )

        await save_kb_file_id(filename, uploaded.id)
        await clear_chat_history(chat_id)

        await message.answer(
            f"{display_name}, файл «{filename}» успешно обновлён в базе знаний.\n"
            f"Удалено старых версий: {deleted_count}.\n"
            f"История этого чата очищена, чтобы бот не тянул старые ответы."
        )

    except Exception as e:
        print(f"DOCUMENT UPDATE ERROR: {type(e).__name__}: {e}", flush=True)

        await message.answer(
            f"{display_name}, не удалось обновить файл «{filename}».\n\n"
            f"Ошибка: {type(e).__name__}\n{e}"
        )

    finally:
        waiting_for_kb_file.discard(chat_id)
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

        if temp_path:
            temp_dir = os.path.dirname(temp_path)
            if os.path.exists(temp_dir):
                try:
                    os.rmdir(temp_dir)
                except Exception:
                    pass


@dp.message(F.text)
async def text_router(message: types.Message):
    chat_id = str(message.chat.id)

    if chat_id in waiting_for_kb_delete:
        await handle_kb_delete_filename(message)
        return

    await handle_question(message)


async def handle_kb_delete_filename(message: types.Message):
    chat_id = str(message.chat.id)

    if not is_admin(message.from_user.id):
        waiting_for_kb_delete.discard(chat_id)
        await message.answer("Удаление доступно только администраторам.")
        return

    filename = (message.text or "").strip()
    display_name = get_display_name(message.from_user)

    if not filename:
        waiting_for_kb_delete.discard(chat_id)
        await message.answer("Имя файла не указано.")
        return

    await message.answer(f"{display_name}, ищу файл «{filename}» в базе знаний...")

    try:
        deleted_count = delete_all_vector_store_files_by_filename(filename)

        if deleted_count == 0:
            await delete_kb_file(filename)
            waiting_for_kb_delete.discard(chat_id)
            await message.answer(
                f"{display_name}, файл «{filename}» не найден в OpenAI Vector Store.\n\n"
                f"Я удалил его из локального списка, чтобы он больше не отображался в /kb_delete."
            )
            return

        await delete_kb_file(filename)
        await clear_chat_history(chat_id)

        await message.answer(
            f"{display_name}, файл «{filename}» удалён из базы знаний.\n"
            f"Удалено версий файла: {deleted_count}.\n"
            f"История этого чата очищена, чтобы бот не тянул старую информацию."
        )

    except Exception as e:
        await message.answer(
            f"{display_name}, не удалось удалить файл «{filename}».\n\n"
            f"Ошибка: {type(e).__name__}\n{e}"
        )

    finally:
        waiting_for_kb_delete.discard(chat_id)


async def handle_question(message: types.Message):
    if not is_allowed(message.from_user.id):
        await message.answer("У вас нет доступа.")
        return

    question = (message.text or "").strip()
    if not question:
        await message.answer("Напиши вопрос текстом.")
        return

    chat_id = str(message.chat.id)
    display_name = get_display_name(message.from_user)

    await save_message(chat_id, "user", question)

    history = await get_recent_messages(chat_id, limit=30)

    input_data = [
        {"role": "system", "content": SYSTEM_INSTRUCTIONS},
        {
            "role": "system",
            "content": f"Пользователя зовут: {display_name}. Обращайся к нему естественно, по имени, без перебора."
        },
        {
            "role": "system",
            "content": (
                "Если последнее сообщение пользователя короткое или начинается с "
                "'а', 'а если', 'а кто', 'а когда', 'а что', "
                "считай его продолжением предыдущего вопроса в этом чате."
            )
        },
        {
            "role": "system",
            "content": (
                "Сначала определи, является ли вопрос продолжением предыдущего диалога. "
                "Потом найди ответ в базе знаний. "
                "Если вопрос неполный, отвечай с учетом предыдущего контекста."
            )
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
                "max_num_results": 10
            }],
        )

        answer = (resp.output_text or "").strip()

        if not answer:
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
    print("BOT STARTING...", flush=True)
    await init_db()
    print("DB INIT OK", flush=True)
    await bot.delete_webhook(drop_pending_updates=True)
    print("WEBHOOK DELETED", flush=True)
    print("POLLING STARTED", flush=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())