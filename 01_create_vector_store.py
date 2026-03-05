import os
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

KB_DIR = Path("kb")

def main():
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Нет OPENAI_API_KEY в .env")

    client = OpenAI(api_key=api_key)

    # 1) создаём vector store
    vs = client.vector_stores.create(name="DVK KB MVP")
    print("VECTOR_STORE_ID:", vs.id)

    # 2) загружаем файлы из kb/
    file_paths = [p for p in KB_DIR.iterdir() if p.is_file()]
    if not file_paths:
        raise RuntimeError("Папка kb пустая. Положи туда 3 файла базы знаний.")

    for p in file_paths:
        print("Uploading:", p.name)
        f = client.files.create(
            file=open(p, "rb"),
            purpose="assistants"
        )
        client.vector_stores.files.create(
            vector_store_id=vs.id,
            file_id=f.id
        )

    print("\nГотово. Теперь скопируй VECTOR_STORE_ID в .env (поле VECTOR_STORE_ID=...)")
    print("Дальше запускай бота.")

if __name__ == "__main__":
    main()