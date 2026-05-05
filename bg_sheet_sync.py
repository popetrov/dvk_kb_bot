import gspread
from google.oauth2.service_account import Credentials

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/19JPbqw9xUvXZi9sRBiYjD70nGCQlMTrYqx3xVcPs4Rg/edit"
SERVICE_ACCOUNT_FILE = "google_service_account.json"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_url(SPREADSHEET_URL)

print("Таблица:", spreadsheet.title)

# берем первую вкладку
worksheet = spreadsheet.get_worksheet(0)

print("Первая вкладка:", worksheet.title)

# читаем строки
rows = worksheet.get_all_values()

print("\nПервые 5 строк таблицы:\n")

for row in rows[:5]:
    print(row)