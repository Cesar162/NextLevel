
from msilib.schema import File
from bs4 import BeautifulSoup
import requests
import sqlite3
from sqlite3 import OperationalError
from datetime import datetime
import csv
from abc import ABC, abstractmethod
import time
from loguru import logger


def yahoo_quote(self, fii):
    fii = fii.replace('/fiis/', '')
    url = 'https://finance.yahoo.com/quote/' + fii + '.SA'
    response = requests.get(url, headers=self.header)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'lxml')
        quotation = soup.find_all('span', {"class": "Trsdu(0.3s) Fw(b) Fz(36px) Mb(-4px) D(ib)"})
        try:
            quotation = quotation[0].text
            return quotation
        except IndexError:
            quotation = '0.0'
            return quotation


def mfinance_quote(fii):
    url = 'https://mfinance.com.br/api/v1/fiis/' + fii
    response = requests.get(url).json()
    quotation = str(response['lastPrice'])
    return [fii, quotation]


def csv_funds(fii):
    funds: list = []
    with open(fii, newline='') as file:
        reader = csv.DictReader(file)
        for row in reader:
            funds.append(row['Codigo'])
    return funds


def mfinance_funds():
    funds: list = []
    url = 'https://mfinance.com.br/api/v1/fiis'
    response = requests.get(url).json()['fiis']
    for fund in response:
        funds.append(fund['symbol'])
    return funds


def quotes_on_yahoo_csv_funds(file: File) -> tuple: #OK
    return ([yahoo_quote(fund) for fund in csv_funds(file)])

def quotes_on_yahoo_mfinance_funds() -> tuple: #NS
    return ([yahoo_quote(fund) for fund in mfinance_funds()])

def quotes_on_mfinance_csv_funds(file: File) -> tuple: #NS
    return ([mfinance_quote(fund) for fund in csv_funds(file)])

def quotes_on_mfinance_mfinance_funds() -> tuple: #OK
    return ([mfinance_quote(fund) for fund in mfinance_funds()])


class Crawler(ABC):
    header = {'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:32.0) Gecko/20100101 Firefox/32.0'}
    funds = []

    @abstractmethod
    def get_funds(self):
        pass

    @abstractmethod
    def get_quotation(self):
        pass


class Crawler_1(Crawler):
    def __init__(self, file_name='fundosListados.csv'):
        self.file_name = file_name

    def get_funds(self):
        return csv_funds(self.file_name)

    def get_quotation(self):
        return quotes_on_yahoo_csv_funds(self.file_name)

    def __str__(self):
        return "Classe Extract que faz uso de planilha e site yahoo finance"

    def __repr__(self):
        return "Classe Extract => Planilha e YahooFinance (API)"


class Crawler_2(Crawler):
    def get_funds(self):
        return mfinance_funds()

    def get_quotation(self):
        return quotes_on_mfinance_mfinance_funds()

    def __str__(self):
        return "Classe Extract que faz uso da API mfinance"

    def __repr__(self):
        return "Classe Extract => API MFinance"


class Writer(ABC):
    @abstractmethod
    def store(self):
        pass


class Writer_1(Writer):
    def __init__(self, db_name='fii.db'):
        self.db_name = db_name
        self._criar_banco_fii(self.db_name)

    def _criar_banco_fii(self, db_name):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute("""
                    CREATE TABLE fii (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        codigo_fii TEXT NOT NULL,
                        preco FLOAT NOT NULL,
                        datetime DATE NOT NULL );
                        """)
        except OperationalError:
            logger.info("Table already created")
        self.connection.close()

    def store(self, quotations):
        logger.debug("Storing...")
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()
        self.cursor.executemany("""
        INSERT INTO quotations (codigo_fii, preco, datetime)
        VALUES (?,?,?)
        """, quotations)
        self.connection.commit()
        self.connection.close()

    def __str__(self):
        return "Classe Loader: Armazenar no banco de dados SQLite"

    def __repr__(self):
        return "Classe Loader: SQLite"


class Writer_2(Writer):
    def __init__(self, file_name='fii.csv'):
        self.file_name = file_name

    def store(self, quotations):
        logger.debug("Storing...")
        header = ['codigo_fii', 'preco', 'datetime']
        with open(self.file_name, 'w') as out:
            csv_out = csv.writer(out)
            csv_out.writerow(header)
            csv_out.writerows(quotations)

    def __str__(self):
        return "Classe Loader: Armazenar em planilhas CSV"

    def __repr__(self):
        return "Classe Loader: Planilhas CSV"


class MeuETL:
    def __init__(self, crawler, writer):
        if not isinstance(crawler, Crawler):
            raise TypeError('Incorrect crawler')
        self.crawler = crawler
        if not isinstance(writer, Writer):
            raise TypeError('Incorrect writer')
        self.writer = writer
        self.funds = self.crawler.get_funds()

    def execute(self):
        logger.debug("Executing...")
        logger.debug("Initializing Quotations...")

        start = time.time() 

        save_quotations = []
        quotation = self.crawler.get_quotation()
        print(type(quotation))
        save_quotations.append((quotation[:][0], quotation[:][1], datetime.now()))
        self.writer.store(save_quotations)

        logger.success("Finished Quotations...")

        end = time.time()
        logger.info(f"--- {end - start} seconds ---")

    def __str__(self):
        return "Classe Meu ETL"

    def __repr__(self):
        return "Classe Meu ETL"


if __name__ == "__main__":
    extrator2 = Crawler_2()

    meu_etl = MeuETL(extrator2, Writer_1())
    meu_etl.execute()
    
    meu_etl = MeuETL(Crawler_1(), Writer_1('fii2.db'))
    meu_etl.execute()

    meu_etl = MeuETL(extrator2, Writer_2())
    meu_etl.execute()