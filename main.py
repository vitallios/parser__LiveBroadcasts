import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('parser.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    'base_url': 'https://srrb.ru/category/translyacii-sportivnyx-sobytij',
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'timeout': 15,
    'max_workers': 4,
    'retry_strategy': {
        'total': 3,
        'backoff_factor': 1,
        'status_forcelist': [500, 502, 503, 504]
    }
}

CATEGORIES = {
    "хоккей": ["ВХЛ", "ХК", "МХЛ", "КХЛ", "ЖХЛ", "хоккей"],
    "баскетбол": ["Баскетбол", "НБА", "Евролига"],
    "теннис": ["ATP", "Теннис", "WTA", "US Open", "Ролан Гаррос"],
    "регби": ["Регби", "Про Д2"],
    "футбол": ["Футбол", "Лига чемпионов", "Премьер-лига"],
    "велоспорт": ["Велоспорт"],
    "гандбол": ["Гандбол"],
    "бокс": ["боксу"],
    "другое": []
}

class SportStreamParser:
    def __init__(self):
        self.session = self._create_session()
        self.strime_list = []
        self.today = datetime.now().date()
        self.today_str = self.today.strftime("%d.%m.%Y")

    def _create_session(self):
        session = requests.Session()
        retry = Retry(
            total=CONFIG['retry_strategy']['total'],
            backoff_factor=CONFIG['retry_strategy']['backoff_factor'],
            status_forcelist=CONFIG['retry_strategy']['status_forcelist']
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update({
            'User-Agent': CONFIG['user_agent'],
            'Accept-Language': 'ru-RU,ru;q=0.9',
            'Accept-Encoding': 'gzip, deflate'
        })
        return session

    def _extract_date(self, title):
        month_map = {
            'января': '01', 'февраля': '02', 'марта': '03',
            'апреля': '04', 'мая': '05', 'июня': '06',
            'июля': '07', 'августа': '08', 'сентября': '09',
            'октября': '10', 'ноября': '11', 'декабря': '12'
        }

        dd_mm_yyyy = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', title)
        if dd_mm_yyyy:
            day, month, year = dd_mm_yyyy.groups()
            return datetime(int(year), int(month), int(day)).date()

        for month_ru, month_num in month_map.items():
            pattern = rf'(\d{{1,2}})\s+{month_ru}\s+(\d{{4}})'
            match = re.search(pattern, title)
            if match:
                day, year = match.groups()
                return datetime(int(year), int(month_num), int(day)).date()

        return None

    def _get_page(self, url):
        try:
            response = self.session.get(url, timeout=CONFIG['timeout'])
            response.encoding = 'utf-8'
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except Exception as e:
            logger.warning(f"Ошибка загрузки {url}: {str(e)}")
            return None

    def _get_stream_info(self, url):
        soup = self._get_page(url)
        if not soup:
            return None, None

        time_text = None
        content = soup.find('div', class_='entry-content')
        if content:
            for p in content.find_all('p'):
                text = p.get_text().lower()
                if any(kw in text for kw in ['прямой эфир', 'начало', 'трансляция', 'мск']):
                    time_text = p.get_text().strip()
                    break

        iframe = soup.find('iframe', {'src': True})
        iframe_html = str(iframe) if iframe else None

        return time_text, iframe_html

    def _clean_title(self, title, date_str, time_str):
        words_to_remove = [
            'Смотреть', 'онлайн', 'трансляция', 'эфир',
            date_str.strftime("%d.%m.%Y"), time_str, 'в', 'мск', '—'
        ]
        words = title.split()
        return ' '.join(w for w in words if w not in words_to_remove)

    def _process_post(self, post):
        try:
            a_tag = post.find('a')
            if not a_tag:
                return None

            link = a_tag.get('href')
            if not link:
                return None

            title = a_tag.get_text().strip()
            event_date = self._extract_date(title)
            
            if not event_date or event_date != self.today:
                logger.debug(f"Пропущена трансляция: {title} (дата: {event_date})")
                return None

            time_text, iframe = self._get_stream_info(link)
            if not time_text or not iframe:
                return None

            time_match = re.search(r'(\d{1,2}:\d{2})', time_text)
            if not time_match:
                return None
            time_str = time_match.group(1)

            clean_title = self._clean_title(title, event_date, time_str)
            if not clean_title:
                return None

            category = "другое"
            for cat, keywords in CATEGORIES.items():
                if any(kw.lower() in clean_title.lower() for kw in keywords):
                    category = cat
                    break

            img = post.find('img', {'src': True})
            img_src = img['src'] if img else None

            return {
                "category": category,
                "name": clean_title,
                "link": iframe,
                "data": event_date.strftime("%Y.%m.%d"),
                "time": time_str,
                "img": img_src,
                "premium": "",
                "active": 0
            }

        except Exception as e:
            logger.error(f"Ошибка обработки поста: {str(e)}")
            return None

    def _generate_telegram_post(self):
        """Генерирует текст поста для Telegram в заданном формате"""
        if not self.strime_list:
            return "🏟️ На сегодня спортивных трансляций не найдено 🏟️"

        # Группируем трансляции по категориям
        categorized = {}
        for item in self.strime_list:
            if item['category'] not in categorized:
                categorized[item['category']] = []
            categorized[item['category']].append(item)

        # Эмодзи для категорий
        category_emojis = {
            "футбол": "⚽️",
            "теннис": "🎾",
            "хоккей": "🏒",
            "баскетбол": "🏀",
            "велоспорт": "🚴",
            "гольф": "🏓",
            "регби": "🏉",
            "гандбол": "🤾",
            "бокс": "🥊",
            "другое": "🏟️"
        }

        # Формируем пост
        post_lines = [
            f"🏟️ Спортивные трансляции на {self.today_str} 🏟️",
            "",
            "📅 Сегодня в эфире:",
            ""
        ]

        # Сортируем категории по порядку из примера
        preferred_order = ["футбол", "теннис", "хоккей", "баскетбол", "велоспорт", "гольф", "регби", "гандбол", "бокс"]
        sorted_categories = sorted(
            categorized.keys(),
            key=lambda x: preferred_order.index(x) if x in preferred_order else len(preferred_order))
        
        for category in sorted_categories:
            emoji = category_emojis.get(category, "🏟️")
            post_lines.append(f"{emoji} {category.capitalize()}")
            
            for item in categorized[category]:
                post_lines.append(f"⏰ {item['time']} - {item['name']}")
            post_lines.append("")  # Пустая строка после категории

        # Добавляем финальные строки
        post_lines.extend([
            "📺 @Live_Strim_bot",
            "",
            "📌 Не пропустите интересные матчи!",
            "#спорт #трансляции #спортивныйкалендарь"
        ])

        return "\n".join(post_lines)

    def parse(self):
        logger.info(f"Запуск парсера спортивных трансляций на {self.today_str}")
        
        try:
            soup = self._get_page(CONFIG['base_url'])
            if not soup:
                raise Exception("Не удалось загрузить главную страницу")

            posts = soup.find_all('article', class_='post')
            if not posts:
                logger.warning("Посты не найдены")
                return

            logger.info(f"Найдено {len(posts)} постов для обработки")

            with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
                results = list(executor.map(self._process_post, posts))
                self.strime_list = [r for r in results if r]

            if self.strime_list:
                # Сохраняем JSON с данными
                with open('strimeList.json', 'w', encoding='utf-8') as f:
                    json.dump(self.strime_list, f, ensure_ascii=False, indent=2)
                logger.info(f"Успешно сохранено {len(self.strime_list)} трансляций на сегодня")

                # Генерируем и сохраняем пост для Telegram
                telegram_post = self._generate_telegram_post()
                with open('telegram_post.txt', 'w', encoding='utf-8') as f:
                    f.write(telegram_post)
                logger.info("Пост для Telegram успешно сгенерирован")
                
                # Выводим пост в консоль для проверки
                print("\n" + "="*50)
                print(telegram_post)
                print("="*50 + "\n")
            else:
                logger.info("Нет трансляций на сегодня")

        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")

if __name__ == '__main__':
    parser = SportStreamParser()
    parser.parse()
