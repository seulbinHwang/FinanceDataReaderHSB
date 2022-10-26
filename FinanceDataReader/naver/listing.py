import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import cloudscraper
import re

try:
    from pandas import json_normalize
except ImportError:
    from pandas.io.json import json_normalize

from FinanceDataReader._utils import (_convert_letter_to_num, _validate_dates)

__tqdm_msg = '''
tqdm not installed. please install as follows

시간이 오래 걸리는 작업을 진행을 표시하기 위해 tqdm 에 의존성이 있습니다.
다음과 같이 tqdm를 설치하세요

C:\> pip insatll tqdm

'''


class NaverStockListing:
    def __init__(self, market):
        self.market = market.upper()

    def read(self):
        verbose, raw = 1, False
        # verbose: 0=미표시, 1=진행막대와 진척율 표시, 2=진행상태 최소표시
        # raw: 원본 데이터를 반환
        exchange_map = {
            'NYSE': 'NYSE',
            'NASDAQ': 'NASDAQ',
            'AMEX': 'AMEX',
            'SSE': 'SHANGHAI',
            'SZSE': 'SHENZHEN',
            'HKEX': 'HONG_KONG',
            'TSE': 'TOKYO',
            'HOSE': 'HOCHIMINH',
        }
        try:
            exchange = exchange_map[self.market]
        except KeyError as e:
            raise ValueError(f'exchange "{self.market}" does not support')

        try:
            from tqdm import tqdm
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(__tqdm_msg)

        url = f'http://api.stock.naver.com/stock/exchange/{exchange}/marketValue?page=1&pageSize=60'
        headers = {'user-agent': 'Mozilla/5.0'}
        try:
            r = requests.get(url, headers=headers)
            jo = json.loads(r.text)
        except JSONDecodeError as e:
            print(r.text)
            raise Exception(f'{r.status_code} "{r.reason}" Server response delayed. Retry later.')

        if verbose == 1:
            t = tqdm(total=jo['totalCount'])

        df_list = []
        for page in range(100):
            url = f'http://api.stock.naver.com/stock/exchange/{exchange}/marketValue?page={page + 1}&pageSize=60'
            try:
                r = requests.get(url, headers=headers)
                jo = json.loads(r.text)
            except JSONDecodeError as e:
                print(r.text)
                raise Exception(f'{r.status_code} "{r.reason}" Server response delayed. Retry later.')

            df = json_normalize(jo['stocks'])
            if not len(df):
                break
            if verbose == 1:
                t.update(len(df))
            elif verbose == 2:
                print('.', end='')
            df_list.append(df)
        if verbose == 1:
            t.close()
            t.clear()
        elif verbose == 2:
            print()
        merged = pd.concat(df_list)
        if raw:
            return merged
        ren_cols = {'symbolCode': 'Symbol',
                    'stockNameEng': 'Name',
                    'industryCodeType.industryGroupKor': 'Industry',
                    'industryCodeType.code': 'IndustryCode'}
        merged = merged[ren_cols.keys()]
        merged.rename(columns=ren_cols, inplace=True)
        merged.reset_index(drop=True, inplace=True)
        return merged

    def read_all(self):
        verbose, raw = 1, False
        # verbose: 0=미표시, 1=진행막대와 진척율 표시, 2=진행상태 최소표시
        # raw: 원본 데이터를 반환
        exchange_map = {
            'NYSE': 'NYSE',
            'NASDAQ': 'NASDAQ',
            'AMEX': 'AMEX',
            'SSE': 'SHANGHAI',
            'SZSE': 'SHENZHEN',
            'HKEX': 'HONG_KONG',
            'TSE': 'TOKYO',
            'HOSE': 'HOCHIMINH',
        }
        try:
            exchange = exchange_map[self.market]
        except KeyError as e:
            raise ValueError(f'exchange "{self.market}" does not support')

        try:
            from tqdm import tqdm
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(__tqdm_msg)

        url = f'http://api.stock.naver.com/stock/exchange/{exchange}/marketValue?page=1&pageSize=60'
        headers = {'user-agent': 'Mozilla/5.0'}
        try:
            r = requests.get(url, headers=headers)
            jo = json.loads(r.text)
        except JSONDecodeError as e:
            print(r.text)
            raise Exception(f'{r.status_code} "{r.reason}" Server response delayed. Retry later.')

        if verbose == 1:
            t = tqdm(total=jo['totalCount'])

        df_list = []
        for page in range(100):
            url = f'http://api.stock.naver.com/stock/exchange/{exchange}/marketValue?page={page+1}&pageSize=60'
            try:
                r = requests.get(url, headers=headers)
                jo = json.loads(r.text)
            except JSONDecodeError as e:
                print(r.text)
                raise Exception(f'{r.status_code} "{r.reason}" Server response delayed. Retry later.')

            df = json_normalize(jo['stocks'])
            ren_cols = {
                'symbolCode': 'Symbol',
                'stockNameEng': 'Name',
                'industryCodeType.industryGroupKor': 'Industry',
                'industryCodeType.code': 'IndustryCode',
            }
            summary_category = {'시가총액': '시가총액'}
            ratios_category = {'PER': 'PER', 'PBR': 'PBR', 'PSR': 'PSR'}
            financials_category = {
                '매출총이익': '매출총이익',
                '자산총계': '총 자산',
                '영업이익': '영업이익',
                '단기차입금': None,
                '장기차입금': None,
                '순이익': '순이익',
                '영업활동': '영업활동 현금흐름',
                '잉여현금': '잉여 현금흐름'
            }
            categories_group = [summary_category, ratios_category, financials_category]  #
            for categories_dict in categories_group:
                for target_category, target_category_name in categories_dict.items():
                    df[f'{target_category}_0'] = None
                    df[f'{target_category}_1'] = None
                    df[f'{target_category}_4'] = None
                    if target_category_name is not None:
                        ren_cols[f'{target_category}_0'] = f'{target_category_name}_0'
                        # ren_cols[f'{target_category}_1'] = f'{target_category_name}_1'
                        # ren_cols[f'{target_category}_4'] = f'{target_category_name}_4'
            for idx in range(len(df)):
                target = df[idx:idx + 1]
                code = str(target['symbolCode'].values[0])
                summary_url = f'https://www.choicestock.co.kr/search/summary/{code}'
                ratios_url = f'https://www.choicestock.co.kr/search/invest/{code}'
                financials_url = f'https://www.choicestock.co.kr/search/financials/{code}/MRQ'
                urls = {
                    summary_url: summary_category.keys(),
                    ratios_url: ratios_category.keys(),
                    financials_url: financials_category.keys()
                }
                # '매출총이익', '자산총계', '영업이익', '단기차입금', '장기차입금', '순이익', '영업활동'
                for url_index, (target_url, target_categories) in enumerate(urls.items()):
                    scraper = cloudscraper.create_scraper(browser={
                        'browser': 'chrome',
                        'platform': 'windows',
                        'mobile': False
                    })
                    ratios_html = scraper.get(target_url).content
                    ratios_soup = BeautifulSoup(ratios_html, 'lxml')

                    # print('ratios_soup:',ratios_soup)
                    if url_index == 0:
                        tables = ratios_soup.find_all('table', {"class": "tableRanking left guide_table"})
                    elif url_index == 1:
                        tables = ratios_soup.find_all('table', {"class": "tableRanking table_search_invest"})
                    elif url_index == 2:
                        tables = ratios_soup.find_all('table', {"class": "tableRanking"})
                    for table in tables:
                        try:
                            trs = table.tbody.find_all('tr')
                            for tr in trs:
                                if url_index < 2:
                                    spans = tr.find_all('span')
                                    if url_index == 0:
                                        category = str(spans[0].text)
                                    else:
                                        for span in spans:
                                            category = str(span.text)
                                elif url_index == 2:
                                    category = tr.find('td').text
                                for target_category in target_categories:
                                    if target_category in category:
                                        tds = tr.find_all('td')
                                        try:
                                            if url_index == 0:
                                                text = str(tds[0].text)
                                                text = float(re.sub(r'[^0-9]', '', text))
                                                df.loc[idx, f'{target_category}_0'] = float(str(text).replace(",", ''))
                                            else:
                                                df.loc[idx, f'{target_category}_0'] = float(
                                                    str(tds[1].text).replace(",", ''))
                                                df.loc[idx, f'{target_category}_1'] = float(
                                                    str(tds[2].text).replace(",", ''))
                                                df.loc[idx, f'{target_category}_4'] = float(
                                                    str(tds[5].text).replace(",", ''))
                                            print('0:', df.loc[idx, f'{target_category}_0'])
                                            print('1:', df.loc[idx, f'{target_category}_1'])
                                            print('4:', df.loc[idx, f'{target_category}_4'])
                                        except:
                                            print(f'{target_category} 데이터가 없습니다:', code)
                        except:
                            pass
                            # try:
                            #     p = ratios_soup.find('p', {"class": "nodata_guide"})
                            #     strongs = p.find_all('strong')
                            #     # /search/summary/GOOGL?pn=&pg=
                            #     for strong in strongs:
                            #         for a in strong.find_all('a', href=True):
                            #             residual_url = a['href']
                            # except:
                            #     pass

            if not len(df):
                break
            if verbose == 1:
                t.update(len(df))
            elif verbose == 2:
                print('.', end='')
            eps = 0.00000001
            df['GP/A_0'] = (df['매출총이익_0'] / (df['자산총계_0']) + eps).astype(float)
            df['GP/A_1'] = (df['매출총이익_1'] / (df['자산총계_1']) + eps).astype(float)
            df['GP/A_4'] = (df['매출총이익_4'] / (df['자산총계_4']) + eps).astype(float)
            ren_cols['GP/A_0'] = 'GP/A_0'
            # ren_cols['GP/A_1'] = 'GP/A_1'
            # ren_cols['GP/A_4'] = 'GP/A_4'

            df['자산성장률(년)'] = (df['자산총계_0'] / (df['자산총계_4'] + eps) - 1.).astype(float)
            ren_cols['자산성장률(년)'] = '자산성장률(년)'

            df['영업이익성장률(분기)'] = (df['영업이익_0'] / (df['영업이익_1'] + eps) - 1.).astype(float)
            ren_cols['영업이익성장률(분기)'] = '영업이익성장률(분기)'
            df['영업이익성장률(년)'] = (df['영업이익_0'] / (df['영업이익_4'] + eps) - 1.).astype(float)
            ren_cols['영업이익성장률(년)'] = '영업이익성장률(년)'

            df['순이익성장률(분기)'] = (df['순이익_0'] / (df['순이익_1'] + eps) - 1.).astype(float)
            ren_cols['순이익성장률(분기)'] = '순이익성장률(분기)'
            df['순이익성장률(년)'] = (df['순이익_0'] / (df['순이익_4'] + eps) - 1.).astype(float)
            ren_cols['순이익성장률(년)'] = '순이익성장률(년)'

            df['차입금_0'] = df['단기차입금_0'] + df['장기차입금_0']
            df['차입금_1'] = df['단기차입금_1'] + df['장기차입금_1']
            df['차입금_4'] = df['단기차입금_4'] + df['장기차입금_4']
            ren_cols['차입금_0'] = '차입금_0'
            # ren_cols['차입금_1'] = '차입금_1'
            # ren_cols['차입금_4'] = '차입금_4'

            a_0 = (df['영업이익_0'] / (df['차입금_0']) + eps).astype(float)
            a_1 = (df['영업이익_1'] / (df['차입금_1']) + eps).astype(float)
            df['(영업이익/차입금)증가율(분기)'] = (a_0 / (a_1 + eps) - 1.).astype(float)
            ren_cols['(영업이익/차입금)증가율(분기)'] = '(영업이익/차입금)증가율(분기)'

            df['차입금증가율(년)'] = (df['차입금_0'] / (df['차입금_1'] + eps) - 1.).astype(float)
            ren_cols['차입금증가율(년)'] = '차입금증가율(년)'
            df['PFCR_0'] = (df['시가총액_0'] / (df['잉여현금_0'] + eps)).astype(float)
            ren_cols['PFCR_0'] = 'PFCR_0'

            df = df.round(3)
            df_list.append(df)
        if verbose == 1:
            t.close()
            t.clear()
        elif verbose == 2:
            print()
        merged = pd.concat(df_list)
        if raw:
            return merged
        merged = merged[ren_cols.keys()]
        merged.rename(columns=ren_cols, inplace=True)
        merged.reset_index(drop=True, inplace=True)
        return merged


class NaverEtfListing:
    def __init__(self):
        pass

    def read(self):
        url = 'https://finance.naver.com/api/sise/etfItemList.nhn'
        df = json_normalize(json.loads(requests.get(url).text), ['result', 'etfItemList'])
        rename_cols = {
            'amonut': 'Amount',
            'changeRate': 'ChangeRate',
            'changeVal': 'Change',
            'etfTabCode': 'Category',
            'itemcode': 'Symbol',
            'itemname': 'Name',
            'marketSum': 'MarCap',
            'nav': 'NAV',
            'nowVal': 'Price',
            'quant': 'Volume',
            'risefall': 'RiseFall',
            'threeMonthEarnRate': 'EarningRate'
        }
        df.rename(columns=rename_cols, inplace=True)
        # 'Symbol', 'Name', 'Price', 'NAV', 'EarningRate', 'Volume',
        # 'Change', 'ChangeRate', 'Amount', 'MarCap', 'EarningRate'
        df = df[['Symbol', 'Name']]
        return df
