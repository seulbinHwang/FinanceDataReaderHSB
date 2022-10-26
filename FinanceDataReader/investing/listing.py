import pandas as pd
import requests
from bs4 import BeautifulSoup
import cloudscraper
import re
from FinanceDataReader._utils import (_convert_letter_to_num, _validate_dates)


class InvestingEtfListing:
    def __init__(self, country):
        self.country = country.upper()

    def read(self):
        country_map = {
            'US': 'usa', 'CN': 'china',
            'HK': 'hong-kong', 'JP': 'japan',
            'UK': 'uk', 'FR': 'france', 'MAJOR': 'major', 'BOND': 'usa'
        }
        if self.country not in country_map.keys():
            msg = "country unsupported. support countries:" + str(list(country_map.keys()))
            raise ValueError(msg)

        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/98.0.4758.109', }
        url = 'https://kr.investing.com/etfs/' + country_map[self.country] + '-etfs'
        if self.country == 'BOND':
            url += '?&asset=2&issuer_filter=0'
        scraper = cloudscraper.create_scraper(
            browser={'browser': 'firefox', 'platform': 'windows', 'mobile': False})
        html = scraper.get(url).content
        soup = BeautifulSoup(html, 'html.parser')
        # r = requests.get(url, headers=headers)
        # soup = BeautifulSoup(r.text, 'lxml') # 'lxml'
        # table 이라는 tag를 찾는다.
        id_lists = ['etfs', 'cr_etf']
        succeed = False
        for id in id_lists:
            table = soup.find('table', id=id)
            if table is None:
                continue
            else:
                succeed = True

            values = []
            trs = table.tbody.find_all('tr')
            for tr in trs:
                tds = tr.find_all('td')
                sym = tds[2].text
                name = tds[1].text
                values.append([sym, name])
            break
        if not succeed:
            raise Exception("StockListing Error!")
        df = pd.DataFrame(values, columns=['Symbol', 'Name'])
        return df

    def read_all(self):
        country_map = {
            'US': 'usa',
            'CN': 'china',
            'HK': 'hong-kong',
            'JP': 'japan',
            'UK': 'uk',
            'FR': 'france',
            'MAJOR': 'major',
            'BOND': 'usa'
        }
        if self.country not in country_map.keys():
            msg = "country unsupported. support countries:" + str(list(country_map.keys()))
            raise ValueError(msg)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/98.0.4758.109',
        }
        url = 'https://kr.investing.com/etfs/' + country_map[self.country] + '-etfs'
        if self.country == 'BOND':
            url += '?&asset=2&issuer_filter=0'
        succeed = False
        while not succeed:
            scraper = cloudscraper.create_scraper(browser={
                'browser': 'firefox',
                'platform': 'windows',
                'mobile': False
            })
            html = scraper.get(url).content
            soup = BeautifulSoup(html, 'html.parser')

            # r = requests.get(url, headers=headers)
            # soup = BeautifulSoup(r.text, 'lxml')
            id_lists = ['etfs', 'cr_etf']
            for id in id_lists:
                table = soup.find('table', id=id)
                if table is None:
                    continue
                else:
                    succeed = True

                values = []
                trs = table.tbody.find_all('tr')
                for tr in trs:
                    tds = tr.find_all('td')
                    sym = tds[2].text
                    name = tds[1].text
                    values.append([sym, name])
                break
        if not succeed:
            raise Exception("StockListing Error!")
        df = pd.DataFrame(values, columns=['Symbol', 'Name'])
        return df
