import os
import json
import hashlib
from pathlib import Path
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
VECTOR_STORE_ID = os.getenv("VECTOR_STORE_ID")

KB_DIR = Path(r"C:\Users\Пользователь\Desktop\dvk_kb_master")
MANIFEST_PATH = Path("kb_manifest.json")

if not OPENAI_API_KEY:
    raise RuntimeError("Нет OPENAI_API_KEY")
if not VECTOR_STORE_ID:
    raise RuntimeError("Нет VECTOR_STORE_ID")
if not KB_DIR.exists():
    raise RuntimeError(f"Папка базы знаний не найдена: {KB_DIR}")

client = OpenAI(api_key=OPENAI_API_KEY)
ALLOWED_EXTENSIONS = {
    ".art", ".bat", ".brf", ".c", ".cls", ".cpp", ".cs", ".css", ".csv",
    ".diff", ".doc", ".docx", ".dot", ".eml", ".es", ".gif", ".go", ".h",
    ".hs", ".htm", ".html", ".hwp", ".hwpx", ".ics", ".ifb", ".java", ".jpeg",
    ".jpg", ".js", ".json", ".keynote", ".ksh", ".ltx", ".mail", ".markdown",
    ".md", ".mht", ".mhtml", ".mjs", ".nws", ".odt", ".pages", ".patch", ".pdf",
    ".php", ".pkl", ".pl", ".pm", ".png", ".pot", ".ppa", ".pps", ".ppt", ".pptx",
    ".pwz", ".py", ".rb", ".rst", ".rtf", ".scala", ".sh", ".shtml", ".srt",
    ".sty", ".svg", ".svgz", ".tar", ".tex", ".text", ".ts", ".txt", ".vcf",
    ".vtt", ".webp", ".wiz", ".xla", ".xlb", ".xlc", ".xlm", ".xls", ".xlsx",
    ".xlt", ".xlw", ".xml", ".yaml", ".yml", ".zip"
}


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(8192)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_manifest(data: dict) -> None:
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    manifest = load_manifest()
    current_files = {}

    for path in KB_DIR.rglob("*"):
        if path.is_file():
            ext = path.suffix.lower()

            if ext not in ALLOWED_EXTENSIONS:
                print(f"Пропущен неподдерживаемый файл: {path.name}")
                continue

            current_files[str(path.relative_to(KB_DIR))] = {
                "path": str(path),
                "hash": file_hash(path)
            }

    deleted_names = set(manifest.keys()) - set(current_files.keys())
    for name in deleted_names:
        old_file_id = manifest[name]["file_id"]
        try:
            client.vector_stores.files.delete(
                vector_store_id=VECTOR_STORE_ID,
                file_id=old_file_id
            )
            print(f"Удалён из vector store: {name}")
        except Exception as e:
            print(f"Ошибка удаления {name}: {e}")
        manifest.pop(name, None)

    for name, info in current_files.items():
        old = manifest.get(name)

        if old and old["hash"] == info["hash"]:
            print(f"Без изменений: {name}")
            continue

        if old:
            try:
                client.vector_stores.files.delete(
                    vector_store_id=VECTOR_STORE_ID,
                    file_id=old["file_id"]
                )
                print(f"Удалена старая версия: {name}")
            except Exception as e:
                print(f"Ошибка удаления старой версии {name}: {e}")

        with open(info["path"], "rb") as f:
            uploaded = client.files.create(
                file=f,
                purpose="assistants"
            )

        client.vector_stores.files.create(
            vector_store_id=VECTOR_STORE_ID,
            file_id=uploaded.id
        )

        manifest[name] = {
            "hash": info["hash"],
            "file_id": uploaded.id
        }

        print(f"Загружен/обновлён: {name}")

    save_manifest(manifest)
    print("Синхронизация завершена.")


if __name__ == "__main__":
    main()