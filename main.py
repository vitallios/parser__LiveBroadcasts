import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

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
    },
    'verify_ssl': False
}

CATEGORIES = {
    "хоккей": ["ВХЛ", "ХК", "МХЛ", "КХЛ", "ЖХЛ", "хоккей"],
    "баскетбол": ["Баскетбол", "НБА", "Евролига"],
    "теннис": ["ATP", "Теннис", "WTA", "US Open", "Ролан Гаррос", "Уимблдон"],
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

    def _get_page(self, url):
        try:
            response = self.session.get(url, timeout=CONFIG['timeout'], verify=CONFIG['verify_ssl'])
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

        # Поиск времени в контенте
        time_text = None
        content = soup.find('div', class_='entry-content')
        if content:
            for p in content.find_all('p'):
                text = p.get_text().lower()
                if any(kw in text for kw in ['прямой эфир', 'начало', 'трансляция', 'мск']):
                    time_match = re.search(r'(\d{1,2}:\d{2})', text)
                    if time_match:
                        time_text = time_match.group(1)
                        break

        # Поиск iframe
        iframe = soup.find('iframe', {'src': True})
        iframe_html = str(iframe) if iframe else None

        return time_text, iframe_html

    def _process_post(self, post):
        try:
            # Извлечение основного контента
            title_tag = post.find('h3').find('a')
            if not title_tag:
                return None
                
            title = title_tag.get_text().strip()
            link = title_tag['href']
            
            # Извлечение даты из метаданных
            date_tag = post.find('span', class_='item-metadata posts-date').find('a')
            if not date_tag:
                return None
                
            try:
                post_date = datetime.strptime(date_tag.get_text().strip(), "%d.%m.%Y").date()
            except ValueError:
                return None
                
            # Проверка что событие сегодня
            if post_date != self.today:
                logger.debug(f"Пропущена трансляция: {title} (дата: {post_date})")
                return None
                
            # Извлечение изображения
            img_tag = post.find('img')
            img_src = img_tag['src'] if img_tag else None
            
            # Извлечение категории
            category_tag = post.find('li', class_='meta-category').find('a')
            category_text = category_tag.get_text().strip() if category_tag else ""
            
            # Получение времени из заголовка
            time_match = re.search(r'в (\d{1,2}:\d{2})', title)
            time_str = time_match.group(1) if time_match else None
            
            # Если время не найдено в заголовке, ищем на странице
            if not time_str:
                time_str, iframe = self._get_stream_info(link)
            else:
                _, iframe = self._get_stream_info(link)
                
            if not iframe:
                return None
                
            # Определение категории
            category = "другое"
            for cat, keywords in CATEGORIES.items():
                if any(kw.lower() in category_text.lower() or kw.lower() in title.lower() for kw in keywords):
                    category = cat
                    break
                    
            # Очистка названия
            clean_title = re.sub(r'\. Прямая трансляция \d{2}\.\d{2}\.\d{4} в \d{1,2}:\d{2}', '', title)
            clean_title = clean_title.replace('Смотреть онлайн:', '').strip()
            
            return {
                "category": category,
                "name": clean_title,
                "link": iframe,
                "data": post_date.strftime("%Y.%m.%d"),
                "time": time_str,
                "img": img_src, 
                "premium": "",
                "active": 0
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки поста: {str(e)}", exc_info=True)
            return None

    def _generate_telegram_post(self):
        if not self.strime_list:
            return "🏟️ На сегодня спортивных трансляций не найдено 🏟️"

        categorized = {}
        for item in self.strime_list:
            if item['category'] not in categorized:
                categorized[item['category']] = []
            categorized[item['category']].append(item)

        category_emojis = {
            "футбол": "⚽️",
            "теннис": "🎾",
            "хоккей": "🏒",
            "баскетбол": "🏀",
            "велоспорт": "🚴",
            "гандбол": "🤾",
            "регби": "🏉",
            "бокс": "🥊",
            "другое": "🏟️"
        }

        post_lines = [
            f"🏟️ Спортивные трансляции на {self.today_str} 🏟️",
            "",
            "📅 Сегодня в эфире:",
            ""
        ]

        preferred_order = ["футбол", "теннис", "хоккей", "баскетбол", "велоспорт", "регби", "гандбол", "бокс"]
        sorted_categories = sorted(
            categorized.keys(),
            key=lambda x: preferred_order.index(x) if x in preferred_order else len(preferred_order))
        
        for category in sorted_categories:
            emoji = category_emojis.get(category, "🏟️")
            post_lines.append(f"{emoji} {category.capitalize()}")
            
            for item in categorized[category]:
                post_lines.append(f"⏰ {item['time']} - {item['name']}")
            post_lines.append("")

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

            posts_container = soup.find('div', id='aft-archive-wrapper')
            if not posts_container:
                raise Exception("Не найден контейнер с постами")

            posts = posts_container.find_all('article', class_='af-sec-post')
            if not posts:
                logger.warning("Посты не найдены")
                return

            logger.info(f"Найдено {len(posts)} постов для обработки")

            with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
                results = list(executor.map(self._process_post, posts))
                self.strime_list = [r for r in results if r]

            if self.strime_list:
                with open('strimeList.json', 'w', encoding='utf-8') as f:
                    json.dump(self.strime_list, f, ensure_ascii=False, indent=2)
                logger.info(f"Успешно сохранено {len(self.strime_list)} трансляций на сегодня")

                telegram_post = self._generate_telegram_post()
                with open('telegram_post.txt', 'w', encoding='utf-8') as f:
                    f.write(telegram_post)
                logger.info("Пост для Telegram успешно сгенерирован")
                
                print("\n" + "="*50)
                print(telegram_post)
                print("="*50 + "\n")
            else:
                logger.info("Нет трансляций на сегодня")

        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)

if __name__ == '__main__':
    parser = SportStreamParser()
    parser.parse()
