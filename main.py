import requests
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging
from typing import Optional, Dict, List, Tuple
import ssl

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SportStreamParser:
    def __init__(self):
        self.session = self._configure_session()
        self.streams = []
        self.today = datetime.now().date()
        self.tomorrow = (datetime.now() + timedelta(days=1)).date()
        self.base_url = "https://srrb.ru/category/translyacii-sportivnyx-sobytij"
        
        # Система категорий с ключевыми словами и эмодзи
        self.categories = {
            "хоккей": {
                "keywords": ["ВХЛ", "ХК", "МХЛ", "КХЛ", "ЖХЛ", "хоккей", "НХЛ"],
                "emoji": "🏒"
            },
            "футбол": {
                "keywords": ["Футбол", "Лига чемпионов", "Премьер-лига", "Чемпионат мира"],
                "emoji": "⚽"
            },
            "теннис": {
                "keywords": ["Теннис", "ATP", "WTA", "US Open", "Ролан Гаррос"],
                "emoji": "🎾"
            },
            "баскетбол": {
                "keywords": ["Баскетбол", "НБА", "Евролига"],
                "emoji": "🏀"
            },
            "Велоспорт": {
                "keywords": ["Велоспорт"],
                "emoji": "🚴‍♀️"
            },
            "другое": {
                "keywords": [],
                "emoji": "🏟"
            }
        }

    def _configure_session(self) -> requests.Session:
        """Настраивает HTTP-сессию с обработкой SSL"""
        session = requests.Session()
        
        # Настройка повторных попыток
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504, 429]
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        # Заголовки
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
        })
        
        # Отключение проверки SSL только для целевого домена
        original_send = session.send
        
        def custom_send(request, **kwargs):
            if "srrb.ru" in request.url:
                kwargs['verify'] = False
            return original_send(request, **kwargs)
        
        session.send = custom_send
        
        return session

    def _parse_date(self, text: str) -> Optional[datetime.date]:
        """Парсит дату из текста в различных форматах"""
        # Формат DD.MM.YYYY
        if match := re.search(r'(\d{2})\.(\d{2})\.(\d{4})', text):
            day, month, year = map(int, match.groups())
            return datetime(year, month, day).date()
        
        # Формат "1 января 2023"
        months = {
            'январ': '01', 'феврал': '02', 'март': '03',
            'апрел': '04', 'мая': '05', 'июн': '06',
            'июл': '07', 'август': '08', 'сентябр': '09',
            'октябр': '10', 'ноябр': '11', 'декабр': '12'
        }
        
        for month_ru, month_num in months.items():
            pattern = rf'(\d{{1,2}})\s*{month_ru}[а-я]*\s*(\d{{4}})'
            if match := re.search(pattern, text, re.IGNORECASE):
                day, year = map(int, match.groups())
                return datetime(year, int(month_num), day).date()
        
        return None

    def _extract_stream_info(self, url: str) -> Tuple[Optional[str], Optional[str]]:
        """Извлекает информацию о времени и iframe трансляции"""
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Поиск времени трансляции
            time_patterns = [
                r'начало\s*в\s*(\d{1,2}:\d{2})',
                r'в\s*(\d{1,2}:\d{2})\s*мск',
                r'трансляция\s*с\s*(\d{1,2}:\d{2})'
            ]
            
            time_text = None
            for p in soup.find_all('p'):
                text = p.get_text().lower()
                for pattern in time_patterns:
                    if match := re.search(pattern, text):
                        time_text = match.group(1)
                        break
                if time_text:
                    break
            
            # Поиск iframe с трансляцией
            iframe = soup.find('iframe', {'src': True})
            iframe_html = str(iframe) if iframe else None
            
            return time_text, iframe_html
            
        except Exception as e:
            logger.warning(f"Ошибка при обработке {url}: {str(e)}")
            return None, None

    def _process_event(self, event_element) -> Optional[Dict]:
        """Обрабатывает отдельное событие"""
        try:
            link = event_element.find('a', href=True)
            if not link:
                return None
                
            title = link.get_text(strip=True)
            event_date = self._parse_date(title)
            
            # Фильтр по дате (сегодня или завтра)
            if not event_date or event_date not in (self.today, self.tomorrow):
                return None
                
            time_text, iframe = self._extract_stream_info(link['href'])
            if not time_text or not iframe:
                return None
                
            # Определение категории (сохраняем оригинальное название)
            category = "другое"
            for cat_name, cat_data in self.categories.items():
                if any(kw.lower() in title.lower() for kw in cat_data['keywords']):
                    category = cat_name
                    break
                    
            # Изображение
            img = event_element.find('img', src=True)
            
            return {
                "category": category,
                "name": title,
                "link": iframe,
                "data": event_date.strftime("%Y.%m.%d"),
                "time": time_text,
                "img": img['src'] if img else None,
                "premium": "",
                "active": 0
            }
            
        except Exception as e:
            logger.error(f"Ошибка обработки события: {str(e)}")
            return None

    def generate_telegram_post(self) -> str:
        """Генерирует текст для Telegram-поста"""
        if not self.streams:
            return "📺 На сегодня спортивных трансляций не найдено"
            
        # Группировка по категориям
        grouped = {}
        for stream in self.streams:
            if stream['category'] not in grouped:
                grouped[stream['category']] = []
            grouped[stream['category']].append(stream)
            
        # Сортировка по времени
        for category in grouped.values():
            category.sort(key=lambda x: x['time'])
            
        # Формирование поста
        lines = [
            f"📺 Спортивные трансляции на {self.today.strftime('%d.%m.%Y')}",
            ""
        ]
        
        for category, streams in grouped.items():
            cat_data = self.categories.get(category, self.categories['другое'])
            lines.append(f"{cat_data['emoji']} {category.upper()}")
            
            for stream in streams:
                lines.append(f"🕒 {stream['time']} - {stream['name']}")
                
            lines.append("")
            
        lines.extend([
            "📺 @Live_Strim_bot",
            "",
            "📌 Не пропустите интересные матчи!",
            "#спорт #трансляции #спортивныйкалендарь #сегодня #трансляцииспорта #прямыетрансляции #спортивныетрансляции #трансляции"
        ])

        
        return "\n".join(lines)

    def run(self):
        """Основной метод запуска парсера"""
        logger.info(f"Запуск парсера на {self.today.strftime('%d.%m.%Y')}")
        
        try:
            # Получение главной страницы
            response = self.session.get(self.base_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            events = soup.find_all('article', class_='post')
            
            if not events:
                logger.warning("События не найдены")
                return
                
            logger.info(f"Найдено {len(events)} событий для обработки")
            
            # Многопоточная обработка
            with ThreadPoolExecutor(max_workers=4) as executor:
                results = list(executor.map(self._process_event, events))
                self.streams = [r for r in results if r]
                
            if self.streams:
                # Сохранение результатов
                with open('strimeList.json', 'w', encoding='utf-8') as f:
                    json.dump(self.streams, f, ensure_ascii=False, indent=2)
                    
                # Генерация поста
                post = self.generate_telegram_post()
                with open('telegram_post.txt', 'w', encoding='utf-8') as f:
                    f.write(post)
                    
                logger.info(f"Успешно обработано {len(self.streams)} трансляций")
                print("\nРезультат:\n")
                print(post)
            else:
                logger.info("Нет актуальных трансляций")
                
        except Exception as e:
            logger.error(f"Критическая ошибка: {str(e)}")

if __name__ == '__main__':
    parser = SportStreamParser()
    parser.run()