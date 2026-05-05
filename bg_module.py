from __future__ import annotations

import os
import re
import asyncio
import json
import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import gspread
from google.oauth2.service_account import Credentials

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

SOURCE_BG_WORKSHEETS = {
    "promos": "Текущие акции ВБЦ",
    "express": "БГ Экспресс",
    "corp": "БГ КОРП",
    "delivery": "Условия доставки",
    "tariffs": "Тарифы",
    "tender_loans": "Тендерные займы",
}

BG_KEYWORDS = [
    "бг", "банковская гарантия", "банковской гарант", "гарантия",
    "44-фз", "44 фз", "223-фз", "223 фз", "615-пп", "615 пп",
    "исполнение", "обеспечение заявки", "заявка", "аванс", "возврат аванса",
    "тендерный займ", "экспресс", "корп", "банк", "банках", "выпустить",
]

PROMO_KEYWORDS = ["акция", "акции", "скидка", "скидки", "спецусловия", "спецпредложения", "вбц", "горячее"]
DELIVERY_KEYWORDS = ["доставка", "условия доставки", "как доставляется", "как привозят", "оригинал", "документы достав"]
TARIFF_KEYWORDS = ["тариф", "тарифы", "ставка", "ставки", "комиссия", "цена", "сколько стоит", "стоимость"]
TENDER_LOAN_KEYWORDS = ["тендерный займ", "займ", "займы", "обеспечение заявки", "деньги на заявку"]
EXPRESS_KEYWORDS = ["экспресс", "бг экспресс", "быстрый выпуск", "срочно", "горит"]
CORP_KEYWORDS = ["корп", "бг корп", "корпоративная"]
RESTRICTION_KEYWORDS = ["ограничения", "стоп", "не берет", "не любят", "сложный клиент", "суды", "блокировки", "усн", "новые компании"]

PRODUCT_ALIASES = {
    "заявка": ["заявка", "обеспечение заявки", "тендерная"],
    "исполнение": ["исполнение", "исполнение контракта"],
    "аванс": ["аванс", "возврат аванса", "авансовая"],
    "платежка": ["платеж", "платежка", "гарантия платежа"],
    "44-фз": ["44-фз", "44 фз"],
    "223-фз": ["223-фз", "223 фз"],
    "615-пп": ["615-пп", "615 пп"],
}

TAX_MODES = ["осн", "осно", "усн"]

SELECTION_SIGNAL_WORDS = [
    "на каких банках", "какие банки", "на каких", "где можем выпустить",
    "куда вести", "что предложить", "основной маршрут", "запасной",
    "подобрать", "подбор", "кейс", "клиент", "дешевле", "подешевле",
    "подороже", "срочно", "выпустить", "запустить",
]


@dataclass
class BGCase:
    raw_text: str
    product: Optional[str] = None
    amount: Optional[float] = None
    term_months: Optional[int] = None
    company_age_months: Optional[int] = None
    revenue: Optional[float] = None
    tax_mode: Optional[str] = None
    has_gov_experience: Optional[bool] = None
    urgent: Optional[bool] = None
    company_is_new: Optional[bool] = None
    wants_cheaper: bool = False
    wants_banks: bool = False
    wants_route: bool = False


@dataclass
class RawMatch:
    sheet_name: str
    row_index: int
    text: str
    score: float
    reasons: List[str] = field(default_factory=list)


@dataclass
class BGReply:
    title: str
    short_answer: str
    full_answer: str
    intent: str
    needs_clarification: bool = False
    clarification_question: Optional[str] = None


class BGSheetsRepository:
    def __init__(self, spreadsheet_id: str, creds_source: str):
        self.spreadsheet_id = spreadsheet_id
        self.creds_source = creds_source
        self._client: Optional[gspread.Client] = None
        self._spreadsheet = None
        self._cache_raw_rows: Dict[str, List[List[str]]] = {}

    def _build_credentials(self) -> Credentials:
        source = (self.creds_source or "").strip()
        if not source:
            raise RuntimeError("Не задан GOOGLE_SERVICE_ACCOUNT_JSON")

        if source.startswith("{"):
            info = json.loads(source)
            return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)

        if source.endswith(".json") and os.path.exists(source):
            return Credentials.from_service_account_file(source, scopes=GOOGLE_SCOPES)

        try:
            decoded = base64.b64decode(source).decode("utf-8")
            if decoded.strip().startswith("{"):
                info = json.loads(decoded)
                return Credentials.from_service_account_info(info, scopes=GOOGLE_SCOPES)
        except Exception:
            pass

        raise RuntimeError("Не удалось прочитать GOOGLE_SERVICE_ACCOUNT_JSON")

    def _connect(self) -> None:
        if self._client is not None:
            return
        credentials = self._build_credentials()
        self._client = gspread.authorize(credentials)
        self._spreadsheet = self._client.open_by_key(self.spreadsheet_id)

    def get_sheet_raw_rows(self, worksheet_name: str) -> List[List[str]]:
        self._connect()
        if worksheet_name in self._cache_raw_rows:
            return self._cache_raw_rows[worksheet_name]
        ws = self._spreadsheet.worksheet(worksheet_name)
        values = ws.get_all_values()
        normalized_rows = [[str(cell).strip() for cell in row] for row in values]
        self._cache_raw_rows[worksheet_name] = normalized_rows
        return normalized_rows


class BGParser:
    @staticmethod
    def is_bg_related(text: str) -> bool:
        text_l = text.lower()
        if any(kw in text_l for kw in BG_KEYWORDS):
            return True
        has_case = bool(BGParser._parse_amount(text_l) or BGParser._parse_term_months(text_l) or BGParser._parse_product(text_l))
        has_bank_context = any(x in text_l for x in ["банк", "банки", "выпустить", "маршрут", "дешевле", "усн"])
        return has_case and has_bank_context

    @staticmethod
    def detect_intent(text: str) -> str:
        text_l = text.lower()
        case = BGParser.extract_case(text)

        selection_hard = (
            any(x in text_l for x in SELECTION_SIGNAL_WORDS)
            or ((case.amount is not None or case.term_months is not None) and (case.product is not None or case.tax_mode is not None or case.company_age_months is not None))
        )
        if selection_hard:
            return "selection"

        if any(kw in text_l for kw in PROMO_KEYWORDS):
            return "promos"
        if any(kw in text_l for kw in DELIVERY_KEYWORDS):
            return "delivery"
        if any(kw in text_l for kw in TENDER_LOAN_KEYWORDS):
            return "tender_loans"
        if any(kw in text_l for kw in EXPRESS_KEYWORDS):
            return "express"
        if any(kw in text_l for kw in CORP_KEYWORDS):
            return "corp"
        if any(kw in text_l for kw in TARIFF_KEYWORDS):
            return "tariffs"
        if any(kw in text_l for kw in RESTRICTION_KEYWORDS):
            return "restrictions"
        return "selection"

    @staticmethod
    def extract_case(text: str) -> BGCase:
        text_l = text.lower()
        case = BGCase(raw_text=text)
        case.product = BGParser._parse_product(text_l)
        case.amount = BGParser._parse_amount(text_l)
        case.term_months = BGParser._parse_term_months(text_l)
        case.company_age_months = BGParser._parse_company_age(text_l)
        case.revenue = BGParser._parse_revenue(text_l)
        case.tax_mode = BGParser._parse_tax_mode(text_l)
        case.has_gov_experience = BGParser._parse_gov_experience(text_l)
        case.urgent = BGParser._parse_urgency(text_l)
        case.wants_cheaper = any(x in text_l for x in ["дешевле", "подешевле", "минимальная ставка", "минимальную ставку", "дешево"])
        case.wants_banks = any(x in text_l for x in ["банк", "банки", "на каких банках", "какие банки"])
        case.wants_route = any(x in text_l for x in ["куда вести", "что предложить", "маршрут", "запасной", "основной"])
        if case.company_age_months is not None:
            case.company_is_new = case.company_age_months < 12
        return case

    @staticmethod
    def _parse_product(text: str) -> Optional[str]:
        for normalized, aliases in PRODUCT_ALIASES.items():
            if any(alias in text for alias in aliases):
                return normalized
        return None

    @staticmethod
    def _parse_amount(text: str) -> Optional[float]:
        patterns = [
            r"(\d+[\d\s]*)\s*(млн|миллион|миллионов)",
            r"сумм[аы]?\s*(\d+[\d\s]*)",
            r"на\s*(\d+[\d\s]*)\s*(₽|руб|рублей)?",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if not m:
                continue
            num = BGParser._safe_number(m.group(1))
            if num is None:
                continue
            unit = m.group(2) if len(m.groups()) > 1 else None
            if unit and isinstance(unit, str) and unit.startswith("м"):
                return num * 1_000_000
            return num
        return None

    @staticmethod
    def _parse_term_months(text: str) -> Optional[int]:
        for pattern in [r"(\d+)\s*(мес|месяц|месяцев)", r"срок\s*(\d+)\s*(мес|месяц|месяцев)?", r"на\s*(\d+)\s*(мес|месяц|месяцев)"]:
            m = re.search(pattern, text)
            if m:
                return int(m.group(1))
        return None

    @staticmethod
    def _parse_company_age(text: str) -> Optional[int]:
        patterns = [
            r"компан[ияи]\s*(\d+)\s*(мес|месяц|месяцев)",
            r"(\d+)\s*(мес|месяц|месяцев)\s*компан",
            r"(\d+)\s*(лет|год|года)\s*компан",
            r"компани[ия]\s*(\d+)\s*(лет|год|года)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text)
            if not m:
                continue
            value = int(m.group(1))
            unit = m.group(2)
            if unit.startswith(("лет", "год")):
                return value * 12
            return value
        return None

    @staticmethod
    def _parse_revenue(text: str) -> Optional[float]:
        for pattern in [r"выручк[аи]?\s*(\d+[\d\s]*)\s*(млн|миллион|миллионов)?", r"оборот[ы]?\s*(\d+[\d\s]*)\s*(млн|миллион|миллионов)?"]:
            m = re.search(pattern, text)
            if not m:
                continue
            num = BGParser._safe_number(m.group(1))
            if num is None:
                continue
            if len(m.groups()) > 1 and m.group(2):
                return num * 1_000_000
            return num
        return None

    @staticmethod
    def _parse_tax_mode(text: str) -> Optional[str]:
        for mode in TAX_MODES:
            if mode in text:
                return "УСН" if mode == "усн" else "ОСНО"
        return None

    @staticmethod
    def _parse_gov_experience(text: str) -> Optional[bool]:
        if "без опыта госконтрактов" in text or "нет опыта госконтрактов" in text:
            return False
        if "опыт госконтрактов есть" in text or "есть опыт госконтрактов" in text:
            return True
        return None

    @staticmethod
    def _parse_urgency(text: str) -> Optional[bool]:
        urgent_words = ["срочно", "горит", "быстро", "сегодня", "за 1 день", "за день"]
        return any(word in text for word in urgent_words)

    @staticmethod
    def _safe_number(value: str) -> Optional[float]:
        try:
            return float(str(value).replace(" ", "").replace(",", "."))
        except Exception:
            return None


class BGService:
    def __init__(self, repo: BGSheetsRepository):
        self.repo = repo

    def _safe_get_raw_rows(self, worksheet_name: str) -> List[List[str]]:
        try:
            return self.repo.get_sheet_raw_rows(worksheet_name)
        except Exception:
            return []

    def handle_message(self, text: str) -> BGReply:
        if not BGParser.is_bg_related(text):
            return BGReply("not_bg", "", "", "not_bg")

        intent = BGParser.detect_intent(text)
        case = BGParser.extract_case(text)

        if intent == "promos":
            return self._handle_promos()
        if intent == "delivery":
            return self._handle_delivery()
        if intent == "tender_loans":
            return self._handle_tender_loans(text)
        if intent == "express":
            return self._handle_express(text, case)
        if intent == "corp":
            return self._handle_corp(text, case)
        if intent == "tariffs":
            return self._handle_tariffs(text, case)
        if intent == "restrictions":
            return self._handle_restrictions(text, case)
        return self._handle_selection(case)

    def _search_sheet(self, sheet_name: str, query_text: str, case: Optional[BGCase] = None, limit: int = 10) -> List[RawMatch]:
        rows = self._safe_get_raw_rows(sheet_name)
        if not rows:
            return []

        query_words = [w for w in re.findall(r"[a-zA-Zа-яА-Я0-9\-]+", query_text.lower()) if len(w) >= 2]
        matches: List[RawMatch] = []

        for idx, row in enumerate(rows, start=1):
            row_text = " | ".join([c for c in row if c]).strip()
            if not row_text:
                continue
            row_l = row_text.lower()
            score = 0.0
            reasons: List[str] = []

            for word in query_words:
                if word in row_l:
                    score += 1.0

            if case and case.product:
                if case.product.lower() in row_l:
                    score += 4
                    reasons.append("совпадение по типу БГ")
                if case.product == "аванс" and any(x in row_l for x in ["аванс", "возврат аванса"]):
                    score += 2

            if case and case.tax_mode and case.tax_mode.lower() in row_l:
                score += 2
                reasons.append("есть совпадение по налоговому режиму")

            if case and case.wants_cheaper and any(x in row_l for x in ["ставк", "тариф", "%", "комисс", "цена"]):
                score += 2.5
                reasons.append("есть ценовой сигнал")

            if case and case.amount is not None:
                if case.amount >= 10_000_000 and sheet_name == SOURCE_BG_WORKSHEETS["corp"]:
                    score += 2.5
                    reasons.append("кейс по сумме тянет в КОРП")
                if any(x in row_l for x in ["до ", "от ", "лимит", "млн"]):
                    score += 0.5

            if case and case.term_months is not None:
                if case.term_months >= 12 and sheet_name == SOURCE_BG_WORKSHEETS["corp"]:
                    score += 1.5
                    reasons.append("срок не короткий")

            if case and case.company_is_new:
                if any(x in row_l for x in ["новые компании", "до 12 мес", "молод"]):
                    score += 2
                    reasons.append("похоже на молодую компанию")
            elif case and case.company_age_months and case.company_age_months >= 24:
                score += 0.8

            if case and case.urgent and any(x in row_l for x in ["быстро", "срочно", "экспресс", "1 день", "2 дня"]):
                score += 2.5
                reasons.append("подходит по срочности")

            if case and case.wants_banks and any(x in row_l for x in ["банк", "банки"]):
                score += 1.5

            if score > 0:
                matches.append(RawMatch(sheet_name, idx, row_text, score, _uniq(reasons)))

        matches.sort(key=lambda x: x.score, reverse=True)
        return matches[:limit]

    def _handle_promos(self) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["promos"], "акции скидки бг вбц", None, 10)
        if not matches:
            msg = "Во вкладке 'Текущие акции ВБЦ' не нашла читаемых данных по акциям."
            return BGReply("bg_promos", msg, msg, "promos")
        lines = ["Что вижу по текущим акциям ВБЦ:"] + [f"- {m.text}" for m in matches[:8]]
        lines += ["", "Как это использовать МОП:", "- сначала предлагай клиенту 1 основной вариант с сильной акцией;", "- рядом держи 1 запасной, если клиент начнет торговаться;", "- не пересылай клиенту всю таблицу, а упакуй в 2–3 понятных варианта."]
        return BGReply("bg_promos", "Нашла актуальные акции по БГ.", "\n".join(lines), "promos")

    def _handle_express(self, text: str, case: BGCase) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["express"], text, case, 8)
        if not matches:
            msg = "Во вкладке 'БГ Экспресс' не нашла явных совпадений. Но этот блок обычно годится, когда клиенту важны скорость и простота захода."
            return BGReply("bg_express", msg, msg, "express")
        lines = ["Что вижу по вкладке 'БГ Экспресс':"] + [f"- {m.text}" for m in matches[:6]]
        lines += ["", "Как это продавать клиенту:", "- продавай как быстрый и понятный маршрут;", "- сразу честно проговаривай, что скорость иногда стоит дороже;", "- особенно уместно, когда клиенту нужно быстро и без долгих согласований."]
        return BGReply("bg_express", "Нашла релевантные данные по БГ Экспресс.", "\n".join(lines), "express")

    def _handle_corp(self, text: str, case: BGCase) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["corp"], text, case, 8)
        if not matches:
            msg = "Во вкладке 'БГ КОРП' не нашла явных совпадений. Но этот блок обычно сильнее на более крупных и не самых простых кейсах."
            return BGReply("bg_corp", msg, msg, "corp")
        lines = ["Что вижу по вкладке 'БГ КОРП':"] + [f"- {m.text}" for m in matches[:6]]
        lines += ["", "Как это продавать клиенту:", "- подавай как более сильный маршрут для суммы, срока и не самого простого кейса;", "- продавай не как 'сложнее', а как 'надежнее и глубже смотрим на кейс';", "- особенно уместно на крупных суммах, авансе и длинном сроке."]
        return BGReply("bg_corp", "Нашла релевантные данные по БГ КОРП.", "\n".join(lines), "corp")

    def _handle_delivery(self) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["delivery"], "доставка оригинал документы", None, 10)
        if not matches:
            msg = "Во вкладке 'Условия доставки' не нашла читаемых данных."
            return BGReply("bg_delivery", msg, msg, "delivery")
        lines = ["Что вижу по условиям доставки:"] + [f"- {m.text}" for m in matches[:8]]
        lines += ["", "Как это использовать МОП:", "- этот блок нужен, чтобы уверенно закрывать организационные вопросы клиента;", "- хорошо использовать, когда клиент уже близок к запуску и спрашивает про оригиналы и сроки."]
        return BGReply("bg_delivery", "Нашла информацию по условиям доставки.", "\n".join(lines), "delivery")

    def _handle_tariffs(self, text: str, case: BGCase) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["tariffs"], text, case, 10)
        if not matches:
            msg = "Во вкладке 'Тарифы' не нашла явных совпадений по запросу."
            return BGReply("bg_tariffs", msg, msg, "tariffs")
        lines = ["Что вижу по тарифам:"] + [f"- {m.text}" for m in matches[:8]]
        lines += ["", "Как это использовать МОП:", "- клиенту лучше давать 1 основной вариант по цене и 1 альтернативу;", "- если клиент давит на цену, продавай не только ставку, но и шанс прохождения плюс срок."]
        return BGReply("bg_tariffs", "Нашла релевантные данные по тарифам.", "\n".join(lines), "tariffs")

    def _handle_tender_loans(self, text: str) -> BGReply:
        matches = self._search_sheet(SOURCE_BG_WORKSHEETS["tender_loans"], text, None, 10)
        if not matches:
            msg = "Во вкладке 'Тендерные займы' не нашла явных совпадений по запросу."
            return BGReply("bg_tender_loans", msg, msg, "tender_loans")
        lines = ["Что вижу по тендерным займам:"] + [f"- {m.text}" for m in matches[:8]]
        lines += ["", "Как это использовать МОП:", "- предлагай как решение в связке с БГ, когда вопрос не только в гарантии, но и в деньгах на вход в тендер."]
        return BGReply("bg_tender_loans", "Нашла релевантные данные по тендерным займам.", "\n".join(lines), "tender_loans")

    def _handle_restrictions(self, text: str, case: BGCase) -> BGReply:
        matches = self._merge_matches(
            self._search_sheet(SOURCE_BG_WORKSHEETS["express"], text, case, 5),
            self._search_sheet(SOURCE_BG_WORKSHEETS["corp"], text, case, 5),
            self._search_sheet(SOURCE_BG_WORKSHEETS["tariffs"], text, case, 5),
        )
        if not matches:
            return self._build_route_only_reply(case, [])
        lines = ["Что вижу по ограничениям / условиям отбора:"] + [f"- [{m.sheet_name}] {m.text}" for m in matches[:8]]
        lines += ["", "Как это использовать МОП:", "- если видишь риск, не пугай клиента, а веди в более рабочий маршрут;", "- задача продавца здесь не перечислить стопы, а выбрать первый эшелон и запасной."]
        return BGReply("bg_restrictions", "Нашла релевантные фрагменты по ограничениям и условиям отбора.", "\n".join(lines), "restrictions")

    def _handle_selection(self, case: BGCase) -> BGReply:
        missing = self._get_missing_minimum_fields(case)
        if missing:
            q = self._build_clarification_question(missing)
            return BGReply("need_more_data", q, q, "selection", True, q)

        express_matches = self._search_sheet(SOURCE_BG_WORKSHEETS["express"], case.raw_text, case, 8)
        corp_matches = self._search_sheet(SOURCE_BG_WORKSHEETS["corp"], case.raw_text, case, 8)
        tariff_matches = self._search_sheet(SOURCE_BG_WORKSHEETS["tariffs"], case.raw_text, case, 8)
        combined = self._merge_matches(express_matches, corp_matches, tariff_matches)

        if not combined:
            return self._build_route_only_reply(case, [])

        best = self._choose_best_match(case, express_matches, corp_matches, tariff_matches, combined)
        alt = self._choose_alt_match(best, combined)
        fast = self._pick_fastest(express_matches, corp_matches)

        lines: List[str] = ["Подбор по БГ готов.", "", "Краткий итог:"]
        if best:
            lines.append(f"🟢 Основной маршрут: [{best.sheet_name}] {best.text}")
        if alt:
            lines.append(f"🟡 Запасной маршрут: [{alt.sheet_name}] {alt.text}")
        if fast and (not best or fast.text != best.text):
            lines.append(f"🔵 Быстрый вариант: [{fast.sheet_name}] {fast.text}")

        bank_lines = self._build_bank_comment(case, combined)
        if bank_lines:
            lines += [""] + bank_lines

        lines += ["", "Разбор кейса:"] + self._case_analysis_lines(case)
        lines += ["", "Как подать клиенту:"] + self._sales_tactics(best, alt, fast, case)
        lines += ["", "Что еще уточнить у клиента:"] + [f"- {q}" for q in self._missing_or_useful_questions(case)]

        return BGReply("bg_selection", "Подобрала маршруты по исходным вкладкам таблицы.", "\n".join(lines), "selection")

    def _build_route_only_reply(self, case: BGCase, matches: List[RawMatch]) -> BGReply:
        route = self._heuristic_route(case)
        lines = [
            "Точного совпадения по сырому тексту вкладок не нашла, но кейс без ответа не оставляю.",
            "",
            "Что делать МОП по этому кейсу:",
            f"🟢 Основной маршрут: {route['main']}",
            f"🟡 Запасной маршрут: {route['alt']}",
            f"🔵 По цене: {route['price']}",
        ]
        if case.wants_banks:
            lines += ["", "По банкам:", "- в исходных строках не нашла уверенного банка, который можно назвать без натяжки;", "- поэтому честно даю не выдуманный банк, а рабочий маршрут: куда вести кейс первым, куда держать запас, где смотреть цену."]
        lines += ["", "Почему такой маршрут:"] + self._case_analysis_lines(case)
        lines += ["", "Как сказать клиенту:"] + route["pitch"]
        lines += ["", "Что уточнить сразу:"] + [f"- {q}" for q in self._missing_or_useful_questions(case)]
        return BGReply("bg_selection", "Не нашла прямого совпадения, но собрала рабочий маршрут для МОП.", "\n".join(lines), "selection")

    def _heuristic_route(self, case: BGCase) -> Dict[str, Any]:
        large = (case.amount or 0) >= 10_000_000
        long_term = (case.term_months or 0) >= 12
        avans = case.product == "аванс"
        urgent = bool(case.urgent)
        cheaper = bool(case.wants_cheaper)

        if avans or large or long_term:
            main = "вести первым эшелоном в БГ КОРП"
            alt = "держать БГ Экспресс как запас, только если нужен более быстрый или упрощенный заход"
        else:
            main = "вести первым эшелоном в БГ Экспресс"
            alt = "держать БГ КОРП как запас, если по экспрессу не будет нужной глубины условий"

        if cheaper:
            price = "параллельно смотреть вкладку 'Тарифы' и торговать не только ставкой, но и вероятностью прохождения"
        else:
            price = "цену смотреть уже после выбора рабочего маршрута, чтобы не покупать клиенту самый дешевый, но слабый вариант"

        pitch = [
            "- Смотрю для вас не просто самую низкую цену, а рабочий маршрут, где есть шанс пройти без лишней потери времени.",
            "- Первым веду кейс по основному маршруту, параллельно держу запасной, чтобы быстро перестроиться.",
        ]
        if cheaper:
            pitch.append("- По цене тоже посмотрю, но не в отрыве от шанса выпуска: дешевый маршрут без прохождения клиенту не поможет.")
        if urgent:
            pitch.append("- Если срок критичен, отдельно проверю быстрый сценарий, даже если он будет чуть дороже.")

        return {"main": main, "alt": alt, "price": price, "pitch": pitch}

    def _choose_best_match(self, case: BGCase, express_matches: List[RawMatch], corp_matches: List[RawMatch], tariff_matches: List[RawMatch], combined: List[RawMatch]) -> Optional[RawMatch]:
        if case.wants_cheaper and tariff_matches:
            non_tariff = next((m for m in combined if m.sheet_name != SOURCE_BG_WORKSHEETS["tariffs"]), None)
            return non_tariff or tariff_matches[0]
        if (case.product == "аванс") or ((case.amount or 0) >= 10_000_000) or ((case.term_months or 0) >= 12):
            return corp_matches[0] if corp_matches else combined[0]
        if case.urgent:
            return express_matches[0] if express_matches else combined[0]
        return combined[0]

    def _choose_alt_match(self, best: Optional[RawMatch], combined: List[RawMatch]) -> Optional[RawMatch]:
        if not best:
            return combined[1] if len(combined) > 1 else None
        for item in combined:
            if (item.sheet_name, item.row_index, item.text) != (best.sheet_name, best.row_index, best.text):
                return item
        return None

    def _build_bank_comment(self, case: BGCase, matches: List[RawMatch]) -> List[str]:
        if not case.wants_banks:
            return []
        bank_rows = [m for m in matches if "банк" in m.text.lower()]
        if not bank_rows:
            return ["По банкам:", "- в исходных совпадениях не вижу банка, который можно уверенно назвать без натяжки;", "- поэтому даю честный маршрут по продуктовым блокам, а не выдумываю банк."]
        return ["По банкам:"] + [f"- [{m.sheet_name}] {m.text}" for m in bank_rows[:4]]

    def _merge_matches(self, *groups: List[RawMatch]) -> List[RawMatch]:
        all_items: List[RawMatch] = []
        seen = set()
        for group in groups:
            for item in group:
                key = (item.sheet_name, item.row_index, item.text)
                if key not in seen:
                    seen.add(key)
                    all_items.append(item)
        all_items.sort(key=lambda x: x.score, reverse=True)
        return all_items[:10]

    def _pick_fastest(self, express_matches: List[RawMatch], corp_matches: List[RawMatch]) -> Optional[RawMatch]:
        return express_matches[0] if express_matches else (corp_matches[0] if corp_matches else None)

    def _get_missing_minimum_fields(self, case: BGCase) -> List[str]:
        missing = []
        if not case.product:
            missing.append("тип БГ")
        if case.amount is None:
            missing.append("сумма")
        if case.term_months is None:
            missing.append("срок")
        return missing

    def _build_clarification_question(self, missing: List[str]) -> str:
        return (
            f"Чтобы сделать нормальный подбор по БГ, мне не хватает: {', '.join(missing)}.\n\n"
            "Напиши одной строкой, например:\n"
            "аванс, 12 млн, 12 мес, УСН, компании 6 лет, нужно дешевле"
        )

    def _case_analysis_lines(self, case: BGCase) -> List[str]:
        lines = []
        if case.product:
            lines.append(f"- тип БГ: {case.product}")
        if case.amount is not None:
            lines.append(f"- сумма: {_format_money(case.amount)}")
        if case.term_months is not None:
            lines.append(f"- срок: {case.term_months} мес")
        if case.tax_mode:
            lines.append(f"- налоговый режим: {case.tax_mode}")
        if case.company_age_months is not None:
            years = round(case.company_age_months / 12, 1)
            lines.append(f"- возраст компании: {years} лет" if case.company_age_months >= 12 else f"- возраст компании: {case.company_age_months} мес")
        if case.product == "аванс":
            lines.append("- авансовая БГ сама по себе уже тянет кейс в более аккуратный и не самый примитивный маршрут")
        if case.amount is not None and case.amount >= 10_000_000:
            lines.append("- сумма уже ощутимая, поэтому КОРП надо держать в приоритете")
        if case.term_months is not None and case.term_months >= 12:
            lines.append("- срок не короткий, это тоже усиливает логику более основательного маршрута")
        if case.tax_mode == "УСН":
            lines.append("- УСН фиксирую как параметр кейса, но не как стоп по умолчанию")
        if case.wants_cheaper:
            lines.append("- клиент явно смотрит на цену, значит нужно дать вариант по цене, но не уронить вероятность выпуска")
        if case.urgent:
            lines.append("- кейс срочный, значит отдельно держим быстрый вариант")
        return lines

    def _sales_tactics(self, best: Optional[RawMatch], alt: Optional[RawMatch], fast: Optional[RawMatch], case: BGCase) -> List[str]:
        lines = []
        if best:
            lines.append(f"- основным продавай маршрут из блока {best.sheet_name}.")
        if alt:
            lines.append(f"- запасным держи маршрут из блока {alt.sheet_name}.")
        if fast:
            lines.append(f"- если клиенту важнее срок, отдельно упакуй быстрый вариант из блока {fast.sheet_name}.")
        lines.append("- не выгружай клиенту всю внутреннюю таблицу; продавай 1 основной вариант, 1 запасной и при необходимости 1 ценовой.")
        if case.wants_cheaper:
            lines.append("- на фразу 'нужно дешевле' отвечай не 'сейчас дам самую низкую', а 'смотрю, где можно сделать дешевле без потери шанса на выпуск'.")
        lines.append("- закрывай разговор не на размышления, а на следующий шаг: документы, запуск, экспресс-проверку.")
        return lines

    def _missing_or_useful_questions(self, case: BGCase) -> List[str]:
        questions = []
        if case.company_age_months is None:
            questions.append("точный возраст компании")
        if case.tax_mode is None:
            questions.append("налоговый режим: УСН или ОСНО")
        if case.revenue is None:
            questions.append("выручка / обороты")
        if case.has_gov_experience is None:
            questions.append("есть ли опыт исполнения госконтрактов")
        if case.urgent is None:
            questions.append("насколько срочно нужен выпуск")
        if not questions:
            questions += ["были ли у клиента раньше банковские гарантии", "есть ли нюансы по компании, которые лучше сразу учесть"]
        return questions


class BGModule:
    def __init__(self, service: BGService):
        self.service = service

    @classmethod
    def from_env(cls) -> "BGModule":
        spreadsheet_id = os.getenv("BG_GOOGLE_SHEET_ID", "").strip()
        creds_source = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "service_account.json").strip()
        if not spreadsheet_id:
            raise RuntimeError("Не задан BG_GOOGLE_SHEET_ID")
        repo = BGSheetsRepository(spreadsheet_id=spreadsheet_id, creds_source=creds_source)
        return cls(BGService(repo))

    async def process_message(self, text: str) -> Optional[str]:
        if not text or not text.strip():
            return None
        reply = await asyncio.to_thread(self.service.handle_message, text)
        return None if reply.intent == "not_bg" else reply.full_answer


def _format_money(value: float) -> str:
    value = float(value)
    if value >= 1_000_000:
        millions = value / 1_000_000
        return f"{int(millions)} млн" if millions.is_integer() else f"{millions:.1f} млн"
    return f"{int(value):,}".replace(",", " ")


def _uniq(items: List[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        key = item.strip().lower()
        if key and key not in seen:
            seen.add(key)
            result.append(item)
    return result


if __name__ == "__main__":
    print("bg_module_mop_v2.py готов.")
