import sqlite3
import re
import requests
from bs4 import BeautifulSoup

class Crawler:
    def __init__(self, dbFileName):
        self.conn = sqlite3.connect(dbFileName)
        self.cursor = self.conn.cursor()
        self.initDB()

    def __del__(self):
        self.conn.close()

    def initDB(self):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS urllist (id INTEGER PRIMARY KEY, url TEXT UNIQUE)')
        self.cursor.execute('CREATE TABLE IF NOT EXISTS wordlist (id INTEGER PRIMARY KEY, word TEXT UNIQUE)')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS wordlocation (
                id INTEGER PRIMARY KEY, 
                urlid INTEGER, 
                wordid INTEGER, 
                location INTEGER,
                FOREIGN KEY(urlid) REFERENCES urllist(id),
                FOREIGN KEY(wordid) REFERENCES wordlist(id)
            )
        ''')
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS linkbetweenurl (
                id INTEGER PRIMARY KEY, 
                fromid INTEGER, 
                toid INTEGER,
                FOREIGN KEY(fromid) REFERENCES urllist(id),
                FOREIGN KEY(toid) REFERENCES urllist(id)
            )
        ''')
        self.conn.commit()

### 2. Методы для работы с текстом
    def getTextOnly(self, soup):
        for time_tag in soup.find_all('time'):
            time_tag.decompose()
        text = soup.get_text(separator=' ')
        return ' '.join(text.split())

    def separateWords(self, text):
        words = re.split(r'\W+', text)
        cleaned_words = [re.sub(r'\d+', '', word.lower()) for word in words if word]
        filtered_words = [word.lower() for word in cleaned_words if re.fullmatch(r'[а-яА-ЯёЁ]+', word)]
        return filtered_words

### 3. Обход страниц и индексация
    def isIndexed(self, url):
        self.cursor.execute("SELECT id FROM urllist WHERE url=?", (url,))
        urlid = self.cursor.fetchone()
        if urlid:
            self.cursor.execute("SELECT * FROM wordlocation WHERE urlid=?", (urlid[0],))
            return self.cursor.fetchone() is not None
        return False

    def addToIndex(self, soup, url):
        if self.isIndexed(url):
            return
        print(f"Индексация {url}")
        text = self.getTextOnly(soup)
        words = self.separateWords(text)
        self.cursor.execute("INSERT OR IGNORE INTO urllist (url) VALUES (?)", (url,))
        urlid = self.cursor.lastrowid
        for i, word in enumerate(words):
            self.cursor.execute("INSERT OR IGNORE INTO wordlist (word) VALUES (?)", (word,))
            wordid = self.cursor.lastrowid
            self.cursor.execute("INSERT INTO wordlocation (urlid, wordid, location) VALUES (?, ?, ?)", (urlid, wordid, i))
        self.conn.commit()

    def crawl(self, urlList, maxDepth=1):
        for currDepth in range(maxDepth):
            newUrlList = []
            for url in urlList:
                try:
                    html_doc = requests.get(url).text
                except:
                    continue
                soup = BeautifulSoup(html_doc, "html.parser")
                self.addToIndex(soup, url)
                links = soup.find_all("a", href=True)
                for link in links:
                    linkUrl = link['href']
                    if linkUrl.startswith('http') and linkUrl not in urlList:
                        self.cursor.execute("INSERT OR IGNORE INTO urllist (url) VALUES (?)", (linkUrl,))
                        newUrlList.append(linkUrl)
            urlList = newUrlList
        print("Индексация завершена.")

### 4. Запуск и мониторинг
    def monitorIndex(self):
        tables = ['urllist', 'wordlist', 'wordlocation', 'linkbetweenurl']
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            print(f"{table}: {self.cursor.fetchone()[0]} записей")

# Пример использования
if __name__ == "__main__":
    crawler = Crawler("searchindex.db")
    seedUrls = ["https://lenta.ru/", "https://rbc.ru/"]
    crawler.crawl(seedUrls, maxDepth=2)
    crawler.monitorIndex()
