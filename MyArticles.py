import requests
import csv
import json
from io import StringIO
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import datetime
import sys
import os
import argparse
import re
import pickle
from functools import lru_cache
import tempfile
import signal

# === Определяем, запущены ли мы из PyCharm ===
def is_running_in_pycharm():
    return 'PYCHARM_HOSTED' in os.environ or 'PYDEVD_LOAD_VALUES_ASYNC' in os.environ

# === Аргументы командной строки ===
parser = argparse.ArgumentParser(description='Загрузка данных статей с Quarry на Викисклад')
parser.add_argument('-q', '--query-id', type=int, default=93243, help='ID запроса Quarry')
parser.add_argument('--test', action='store_true', help='Тестовый режим без загрузки')
parser.add_argument('-v', '--verbose', action='store_true', help='Подробный вывод')
parser.add_argument('-w', '--workers', type=int, default=10, help='Количество параллельных потоков')
parser.add_argument('--no-cache', action='store_true', help='Не использовать кеширование')
# Добавляем новый аргумент для принудительного пересчета prose_size
parser.add_argument('--force-recalculate', action='store_true', help='Принудительный пересчет readable prose size')
parser.add_argument('--no-force-recalculate', action='store_true', help='Отключить принудительный пересчет readable prose size')
args = parser.parse_args()

# === Конфигурация ===
QUERY_ID = args.query_id
COMMONS_DATA_TITLE = "Data:Voyagerim/myarticles.tab"
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
WIKIPEDIA_API = "https://ru.wikipedia.org/w/api.php"
USERNAME = "Voyagerim"
PASSWORD = "mailmans1980"
SUMMARY = "Автоматическое обновление данных через Quarry"
LOGFILE = "upload_log.txt"
TEST_MODE = args.test
VERBOSE = args.verbose
MAX_WORKERS = args.workers
USE_CACHE = not args.no_cache

# Установка FORCE_RECALCULATE в зависимости от условий:
# 1. Если указан --no-force-recalculate, то выключаем принудительный пересчет
# 2. Если указан --force-recalculate, то включаем принудительный пересчет
# 3. Если запускаем из PyCharm без параметров, то включаем принудительный пересчет
# 4. В остальных случаях - выключен
if args.no_force_recalculate:
    FORCE_RECALCULATE = False
elif args.force_recalculate:
    FORCE_RECALCULATE = True
elif is_running_in_pycharm() and len(sys.argv) == 1:
    FORCE_RECALCULATE = True
    print("Запуск из PyCharm без параметров: автоматически включен режим принудительного пересчета")
else:
    FORCE_RECALCULATE = False

CACHE_DIR = os.path.join(tempfile.gettempdir(), "wiki_cache")
CACHE_FILE = os.path.join(CACHE_DIR, f"cache_{QUERY_ID}.pkl")

if USE_CACHE and not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

# === Кеширование данных ===
cache = {}
if USE_CACHE and os.path.exists(CACHE_FILE) and not FORCE_RECALCULATE:
    try:
        with open(CACHE_FILE, 'rb') as f:
            cache = pickle.load(f)
    except Exception as e:
        print(f"Ошибка загрузки кеша: {e}")
        cache = {}
else:
    # Если включен режим принудительного пересчета, очищаем кеш
    if FORCE_RECALCULATE:
        log_message = "⚠️ Включен режим принудительного пересчета. Кеш не используется."
        print(log_message)
        if os.path.exists(CACHE_FILE):
            try:
                os.remove(CACHE_FILE)
                print(f"Удален файл кеша: {CACHE_FILE}")
            except Exception as e:
                print(f"Не удалось удалить файл кеша: {e}")
        cache = {}


def save_cache():
    if USE_CACHE and not FORCE_RECALCULATE:
        try:
            with open(CACHE_FILE, 'wb') as f:
                pickle.dump(cache, f)
        except Exception as e:
            print(f"Ошибка сохранения кеша: {e}")


# === Логирование ===
def log(message, level="INFO"):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] [{level}] {message}"
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(log_entry + "\n")
    print(log_entry)


def log_debug(message):
    if VERBOSE:
        log(message, "DEBUG")


# === Сессии и подключения ===
# Создаем общую сессию для повторного использования соединений
wikipedia_session = requests.Session()
commons_session = requests.Session()

# Настраиваем общие заголовки
user_agent = f'VoyagerimBot/1.0 (User:Voyagerim; quarry-id:{QUERY_ID})'
wikipedia_session.headers.update({'User-Agent': user_agent})
commons_session.headers.update({'User-Agent': user_agent})


# === Обработка сигнала Ctrl+C для корректного завершения ===
def setup_signal_handler():
    def signal_handler(sig, frame):
        log("⚠️ Получен сигнал прерывания. Сохраняем кеш и завершаем работу...")
        save_cache()
        log("Кеш сохранен. Завершение работы.")
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


# === Загрузка последнего результата Quarry ===
def run_quarry_query(query_id):
    log("▶ Загружаем результаты последнего выполнения запроса на Quarry...")
    tsv_url = f"https://quarry.wmcloud.org/query/{query_id}/result/latest/0/tsv"
    try:
        with tqdm(total=1, desc="Загрузка данных Quarry", unit="запрос") as pbar:
            r = requests.get(tsv_url)
            r.raise_for_status()
            quarry_rows = list(csv.DictReader(StringIO(r.text), delimiter="\t"))
            pbar.update(1)
        log(f"📊 Получено {len(quarry_rows)} строк данных")
        return quarry_rows
    except Exception as e:
        log(f"❌ Ошибка загрузки TSV: {e}", "ERROR")
        return None


# === Расчёт readable prose size ===
# Удаляем декоратор lru_cache, чтобы не кешировать результаты в памяти
# при включенном режиме принудительного пересчета
def get_prose_size(title):
    # Проверяем кеш только если не включен режим принудительного пересчета
    cache_key = f"prose_size:{title}"
    if USE_CACHE and not FORCE_RECALCULATE and cache_key in cache:
        log_debug(f"Используем кешированный prose_size для {title}: {cache[cache_key]}")
        return cache[cache_key]

    try:
        log_debug(f"Запрашиваем данные для расчёта readable prose size для '{title}'")
        r = wikipedia_session.get(WIKIPEDIA_API, params={
            "action": "parse",
            "page": title,
            "prop": "text",
            "format": "json",
            "formatversion": "2"
        })
        r.raise_for_status()
        data = r.json()

        if "error" in data:
            log_debug(f"Не удалось получить текст для '{title}': {data['error'].get('info', 'неизвестная ошибка')}")
            return 0

        # Проверяем структуру ответа
        if "parse" not in data or "text" not in data["parse"]:
            log_debug(f"Некорректная структура данных для '{title}': отсутствует parse.text")
            return 0

        html = data["parse"]["text"]
        if not html:
            log_debug(f"Пустой HTML для '{title}'")
            return 0

        # Обрабатываем HTML с BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Удаляем ненужные элементы перед обработкой
        for tag in soup.select('table, .navbox, .reference, script, style, .mw-editsection, .noprint'):
            tag.decompose()

        # Ищем параграфы и считаем их текстовый размер
        paragraphs = soup.find_all("p")
        if not paragraphs:
            log_debug(f"Не найдено параграфов в '{title}'")
            return 0

        text = " ".join(p.get_text().strip() for p in paragraphs)
        result = len(text)

        log_debug(f"Вычислен readable prose size для '{title}': {result} символов")

        # Сохраняем в кеш, только если не включен режим принудительного пересчета
        if USE_CACHE and not FORCE_RECALCULATE:
            cache[cache_key] = result
            log_debug(f"Сохранен в кеш prose_size для {title}: {result}")

        return result
    except Exception as e:
        log(f"⚠️ Ошибка prose_size для {title}: {e}", "WARNING")
        import traceback
        log_debug(f"Трассировка: {traceback.format_exc()}")
        return 0


# === Определение качества статьи ===
# Также убираем lru_cache для качества статьи при принудительном пересчете
def get_quality(title):
    # Проверяем кеш только если не включен режим принудительного пересчета
    cache_key = f"quality:{title}"
    if USE_CACHE and not FORCE_RECALCULATE and cache_key in cache:
        return cache[cache_key]

    try:
        r = wikipedia_session.get(WIKIPEDIA_API, params={
            "action": "parse",
            "page": title,
            "prop": "wikitext",
            "format": "json",
            "formatversion": "2"
        })
        r.raise_for_status()
        data = r.json()
        wikitext = data.get("parse", {}).get("wikitext", "").lower()

        # Распознавание шаблонов с параметрами
        result = ""
        if re.search(r"\{\{\s*(subst:\s*)?избранная\s+статья(\|[^}]*)?}}", wikitext):
            result = "featured_article"
        elif re.search(r"\{\{\s*(subst:\s*)?избранный\s+список(\|[^}]*)?}}", wikitext):
            result = "featured_list"
        elif re.search(r"\{\{\s*(subst:\s*)?избранный\s+список\s+или\s+портал(\|[^}]*)?}}", wikitext):
            if re.search(r"тип\s*=\s*список", wikitext):
                result = "featured_list"
        elif re.search(r"\{\{\s*(subst:\s*)?хорошая\s+статья(\|[^}]*)?}}", wikitext):
            result = "good"
        elif re.search(r"\{\{\s*(subst:\s*)?добротная\s+статья(\|[^}]*)?}}", wikitext):
            result = "b"

        # Сохраняем в кеш только если не включен режим принудительного пересчета
        if USE_CACHE and not FORCE_RECALCULATE:
            cache[cache_key] = result

        return result
    except Exception as e:
        log(f"⚠️ Ошибка определения качества для {title}: {e}", "WARNING")
        return ""


# === Пакетная загрузка данных ===
def batch_get_wikitext(titles):
    """Получает wikitext для нескольких страниц за один запрос"""
    if not titles:
        return {}

    batch_size = 50  # API ограничение
    results = {}

    for i in range(0, len(titles), batch_size):
        batch = titles[i:i + batch_size]
        try:
            r = wikipedia_session.get(WIKIPEDIA_API, params={
                "action": "query",
                "titles": "|".join(batch),
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
                "format": "json"
            })
            r.raise_for_status()
            data = r.json()

            if "query" in data and "pages" in data["query"]:
                for page_id, page_data in data["query"]["pages"].items():
                    if "revisions" in page_data and page_data["revisions"]:
                        title = page_data.get("title", "")
                        content = page_data["revisions"][0].get("slots", {}).get("main", {}).get("*", "")
                        results[title] = content
        except Exception as e:
            log(f"⚠️ Ошибка пакетной загрузки: {e}", "WARNING")

    return results


# === Загрузка существующих данных с Commons ===
def load_existing_data():
    log("▶ Загружаем текущие данные с Commons...")
    existing_map = {}
    try:
        with tqdm(total=1, desc="Загрузка с Commons", unit="запрос") as pbar:
            r = commons_session.get(COMMONS_API, params={
                "action": "query",
                "titles": COMMONS_DATA_TITLE,
                "prop": "revisions",
                "rvslots": "main",
                "rvprop": "content",
                "format": "json"
            })
            r.raise_for_status()
            pages = r.json()["query"]["pages"]
            page_id = next(iter(pages))
            pbar.update(1)

        if int(page_id) < 0:
            log("ℹ️ Страница данных на Commons не существует. Будет создана новая.")
            return {}

        content = pages[page_id].get("revisions", [{}])[0].get("slots", {}).get("main", {}).get("*", "")
        if not content.strip():
            return {}

        json_data = json.loads(content)
        if "data" in json_data and isinstance(json_data["data"], list):
            field_names = [f["name"] for f in json_data.get("schema", {}).get("fields", [])]

            # Показываем прогресс бар при обработке существующих данных
            with tqdm(total=len(json_data["data"]), desc="Обработка существующих записей", unit="запись") as pbar:
                for row_data in json_data["data"]:
                    if isinstance(row_data, list) and len(row_data) == len(field_names):
                        row_obj = {field_names[i]: row_data[i] for i in range(len(field_names))}
                        existing_map[row_obj["page_title"]] = row_obj
                    pbar.update(1)

            log(f"📋 Загружено {len(existing_map)} существующих записей")
    except Exception as e:
        log(f"⚠️ Ошибка загрузки/разбора Commons: {e}", "WARNING")
    return existing_map


# === Загрузка данных на Commons ===
def upload_data_to_commons(session, csrf_token, updated_rows):
    log(f"▶ Загружаем {len(updated_rows)} записей на Commons...")

    schema_fields = [
        {"name": "page_title", "type": "string"},
        {"name": "created", "type": "string"},
        {"name": "size", "type": "number"},
        {"name": "prose_size", "type": "number"},
        {"name": "redlinks", "type": "number"},
        {"name": "is_disambiguation", "type": "number"},
        {"name": "comment", "type": "string"},
        {"name": "quality", "type": "string"}
    ]

    # Показываем прогресс при подготовке данных
    tabular_data = []
    with tqdm(total=len(updated_rows), desc="Подготовка данных для загрузки", unit="запись") as pbar:
        for row in updated_rows:
            tabular_data.append([
                row["page_title"],
                row["created"],
                int(float(row["size"])),
                int(row["prose_size"]),
                int(float(row["redlinks"])),
                int(row["is_disambiguation"]),
                row["comment"],
                row["quality"]
            ])
            pbar.update(1)

    upload_data = {
        "license": "CC0-1.0",
        "description": {"en": "Articles created by Voyagerim", "ru": "Статьи, созданные участником Voyagerim"},
        "sources": "Imported from Quarry query results",
        "schema": {"fields": schema_fields},
        "data": tabular_data
    }

    # Прогресс при сериализации (может занять время для больших данных)
    with tqdm(total=1, desc="Сериализация JSON", unit="файл") as pbar:
        upload_text = json.dumps(upload_data, ensure_ascii=False)
        pbar.update(1)

    if TEST_MODE:
        log("🧪 Тестовый режим: загрузка пропущена")
        log_debug(json.dumps(tabular_data[:3], ensure_ascii=False, indent=2))
        return True

    edit_params = {
        "action": "edit",
        "title": COMMONS_DATA_TITLE,
        "text": upload_text,
        "token": csrf_token,
        "format": "json",
        "summary": SUMMARY,
        "contentmodel": "Tabular.JsonConfig"
    }

    try:
        with tqdm(total=1, desc="Загрузка на Commons", unit="запрос") as pbar:
            r = session.post(COMMONS_API, data=edit_params)
            r.raise_for_status()
            response = r.json()
            pbar.update(1)

        if "edit" in response and response["edit"].get("result") == "Success":
            log(f"✅ Успешно загружено {len(tabular_data)} записей!")
            log(f"🔗 https://commons.wikimedia.org/wiki/{COMMONS_DATA_TITLE}")
            return True
        else:
            log(f"❌ Ошибка загрузки: {json.dumps(response, indent=2)}", "ERROR")
            return False
    except Exception as e:
        log(f"❌ Ошибка загрузки на Commons: {e}", "ERROR")
        return False


# === Аутентификация на Commons ===
def authenticate_commons():
    log("▶ Аутентификация на Commons...")
    session = commons_session
    try:
        r1 = session.get(COMMONS_API, params={"action": "query", "meta": "tokens", "type": "login", "format": "json"})
        r1.raise_for_status()
        login_token = r1.json()["query"]["tokens"]["logintoken"]

        login_response = session.post(COMMONS_API, data={
            "action": "login",
            "lgname": USERNAME,
            "lgpassword": PASSWORD,
            "lgtoken": login_token,
            "format": "json"
        })
        login_response.raise_for_status()
        if login_response.json().get("login", {}).get("result") != "Success":
            log("❌ Ошибка входа", "ERROR")
            return None, None

        r2 = session.get(COMMONS_API, params={"action": "query", "meta": "tokens", "format": "json"})
        r2.raise_for_status()
        csrf_token = r2.json()["query"]["tokens"]["csrftoken"]

        log("✅ Аутентификация успешна")
        return session, csrf_token
    except Exception as e:
        log(f"❌ Ошибка аутентификации: {e}", "ERROR")
        return None, None


# === Обработка данных статей ===
def process_article_data(quarry_rows, existing_map):
    log("▶ Обрабатываем данные статей...")

    # Если включен режим принудительного пересчета, выводим уведомление
    if FORCE_RECALCULATE:
        log("⚠️ Включен режим принудительного пересчета prose_size. Кеш не используется.")

    # Предварительно получаем качество для всех статей пакетами
    def prepare_quality_data():
        all_titles = [row["page_title"] for row in quarry_rows]

        # Разбиваем на блоки для предварительного пакетного получения качества
        batch_size = min(50, MAX_WORKERS * 5)  # Оптимальное число для API
        batch_titles = []
        total_batches = (len(all_titles) + batch_size - 1) // batch_size

        log(f"Предзагрузка данных для {len(all_titles)} статей...")
        with tqdm(total=total_batches, desc="Предзагрузка", unit="пакет") as pbar:
            for i in range(0, len(all_titles), batch_size):
                batch_titles = all_titles[i:i + batch_size]
                # Предварительная загрузка викитекста для определения качества
                batch_get_wikitext(batch_titles)
                pbar.update(1)

    # Предварительная загрузка данных
    prepare_quality_data()

    def fetch_row_data(row):
        title = row["page_title"]
        comment = existing_map.get(title, {}).get("comment", "")

        # Получаем prose_size с отметкой времени для отслеживания
        start_time = time.time()
        prose_size = get_prose_size(title)
        elapsed = time.time() - start_time

        # В режиме принудительного пересчета показываем информацию о каждом расчете
        if FORCE_RECALCULATE:
            log_debug(f"ПЕРЕСЧЕТ prose_size={prose_size} для '{title}' за {elapsed:.3f} сек")
        else:
            log_debug(f"Получен prose_size={prose_size} для '{title}' за {elapsed:.3f} сек")

        is_disambiguation = row.get("is_disambiguation", 0)
        quality = get_quality(title)

        return {
            "page_title": title,
            "created": row["created"],
            "size": row.get("size", 0),
            "prose_size": prose_size,
            "redlinks": row.get("redlinks", 0),
            "is_disambiguation": is_disambiguation,
            "comment": comment,
            "quality": quality
        }

    updated_rows = []
    start_time = time.time()

    log(f"Обработка {len(quarry_rows)} статей...")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_row_data, row) for row in quarry_rows]

        # Расширенный progress bar с дополнительной статистикой
        with tqdm(total=len(futures), desc="Обработка статей", unit="статья",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
            for f in as_completed(futures):
                try:
                    result = f.result()
                    updated_rows.append(result)

                    # Дополнительно показываем текущую скорость и статистику prose_size
                    if len(updated_rows) % 10 == 0:
                        current_speed = len(updated_rows) / (time.time() - start_time)
                        avg_prose_size = sum(row["prose_size"] for row in updated_rows) / len(
                            updated_rows) if updated_rows else 0
                        pbar.set_postfix(скорость=f"{current_speed:.2f} ст/сек", сред_размер=f"{avg_prose_size:.1f}")
                except Exception as e:
                    log(f"⚠️ Ошибка обработки: {e}", "WARNING")
                    import traceback
                    log_debug(f"Трассировка: {traceback.format_exc()}")
                pbar.update(1)

    elapsed = time.time() - start_time
    # Считаем статистику по prose_size
    avg_prose = sum(row["prose_size"] for row in updated_rows) / len(updated_rows) if updated_rows else 0
    max_prose = max((row["prose_size"] for row in updated_rows), default=0)
    min_prose = min((row["prose_size"] for row in updated_rows if row["prose_size"] > 0), default=0)
    zero_prose = sum(1 for row in updated_rows if row["prose_size"] == 0)

    log(f"✅ Обработано {len(updated_rows)} статей за {elapsed:.2f} секунд ({len(updated_rows) / elapsed:.2f} статей/сек)")
    log(f"📊 Статистика prose_size: среднее={avg_prose:.1f}, мин={min_prose}, макс={max_prose}, нули={zero_prose}")

    # Сохраняем кеш после обработки, если не включен режим принудительного пересчета
    if not FORCE_RECALCULATE:
        save_cache()

    return updated_rows


# === Главная функция ===
def main():
    # Настраиваем обработку сигналов для корректного завершения
    setup_signal_handler()

    log(f"=== Запуск скрипта {os.path.basename(__file__)} ===")
    log(f"Используется запрос Quarry: {QUERY_ID}")
    log(f"Параллельных потоков: {MAX_WORKERS}")
    log(f"Использование кеша: {USE_CACHE}")
    log(f"Принудительный пересчет: {FORCE_RECALCULATE}")

    if TEST_MODE:
        log("🧪 Запуск в тестовом режиме")

    # Показываем общий прогресс-бар для всего процесса
    total_steps = 4  # Загрузка из Quarry, загрузка с Commons, обработка, выгрузка на Commons
    with tqdm(total=total_steps, desc="Общий прогресс", position=0,
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]") as main_pbar:

        quarry_rows = run_quarry_query(QUERY_ID)
        main_pbar.update(1)

        if not quarry_rows:
            log("❌ Не удалось получить данные из Quarry. Завершение работы.", "ERROR")
            return 1

        existing_map = load_existing_data()
        main_pbar.update(1)

        updated_rows = process_article_data(quarry_rows, existing_map)
        main_pbar.update(1)

        if not updated_rows:
            log("❌ Нет данных для загрузки. Завершение работы.", "ERROR")
            return 1

        session, csrf_token = authenticate_commons()
        if not session or not csrf_token:
            return 1

        result = upload_data_to_commons(session, csrf_token, updated_rows)
        main_pbar.update(1)

    log("=== Завершение работы скрипта ===")
    return 0 if result else 1


if __name__ == "__main__":
    sys.exit(main())