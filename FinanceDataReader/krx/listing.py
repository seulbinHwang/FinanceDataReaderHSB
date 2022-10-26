import io
import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
import json
import ssl
import re
from selenium import webdriver
from selenium.webdriver.common.by import By

try:
    from pandas import json_normalize
except ImportError:
    from pandas.io.json import json_normalize


class KrxStockListing:
    def __init__(self, market):
        self.market = market

    def read(self):
        # KRX 상장회사목록
        # For mac, SSL CERTIFICATION VERIFICATION ERROR
        ssl._create_default_https_context = ssl._create_unverified_context

        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        df_listing = pd.read_html(url, header=0, flavor='bs4', encoding='EUC-KR')[0]
        cols_ren = {'회사명': 'Name', '종목코드': 'Symbol', '업종': 'Sector', '주요제품': 'Industry',
                    '상장일': 'ListingDate', '결산월': 'SettleMonth', '대표자명': 'Representative',
                    '홈페이지': 'HomePage', '지역': 'Region', }
        df_listing = df_listing.rename(columns=cols_ren)
        df_listing['Symbol'] = df_listing['Symbol'].apply(lambda x: '{:06d}'.format(x))
        df_listing['ListingDate'] = pd.to_datetime(df_listing['ListingDate'])

        # KRX 주식종목검색
        data = {'bld': 'dbms/comm/finder/finder_stkisu', }
        r = requests.post('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', data=data)

        jo = json.loads(r.text)
        df_finder = json_normalize(jo, 'block1')

        # full_code, short_code, codeName, marketCode, marketName, marketEngName, ord1, ord2
        df_finder = df_finder.rename(columns={
            'full_code': 'FullCode',
            'short_code': 'Symbol',
            'codeName': 'Name',
            'marketCode': 'MarketCode',
            'marketName': 'MarketName',
            'marketEngName': 'Market',
            'ord1': 'Ord1',
            'ord2': 'Ord2',
        })

        # 상장회사목록, 주식종목검색 병합
        df_left = df_finder[['Symbol', 'Market', 'Name']]
        df_right = df_listing[
            ['Symbol', 'Sector', 'Industry', 'ListingDate', 'SettleMonth', 'Representative', 'HomePage', 'Region']]

        df_master = pd.merge(df_left, df_right, how='left', left_on='Symbol', right_on='Symbol')
        if self.market in ['KONEX', 'KOSDAQ', 'KOSPI']:
            return df_master[df_master['Market'] == self.market]
        return df_master

    def read_all(self):
        # KRX 상장회사목록
        # For mac, SSL CERTIFICATION VERIFICATION ERROR
        ssl._create_default_https_context = ssl._create_unverified_context

        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13'
        df_listing = pd.read_html(url, header=0, flavor='bs4', encoding='EUC-KR')[0]
        cols_ren = {
            '회사명': 'Name',
            '종목코드': 'Symbol',
            '업종': 'Sector',
            '주요제품': 'Industry',
            '상장일': 'ListingDate',
            '결산월': 'SettleMonth',
            '대표자명': 'Representative',
            '홈페이지': 'HomePage',
            '지역': 'Region',
        }
        df_listing = df_listing.rename(columns=cols_ren)
        df_listing['Symbol'] = df_listing['Symbol'].apply(lambda x: '{:06d}'.format(x))
        df_listing['ListingDate'] = pd.to_datetime(df_listing['ListingDate'])

        # KRX 주식종목검색
        data = {
            'bld': 'dbms/comm/finder/finder_stkisu',
        }
        r = requests.post('http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd', data=data)

        jo = json.loads(r.text)
        df_finder = json_normalize(jo, 'block1')

        # full_code, short_code, codeName, marketCode, marketName, marketEngName, ord1, ord2
        df_finder = df_finder.rename(
            columns={
                'full_code': 'FullCode',
                'short_code': 'Symbol',
                'codeName': 'Name',
                'marketCode': 'MarketCode',
                'marketName': 'MarketName',
                'marketEngName': 'Market',
                'ord1': 'Ord1',
                'ord2': 'Ord2',
            })

        # 상장회사목록, 주식종목검색 병합
        df_left = df_finder[['Symbol', 'Market', 'Name']]
        df_right = df_listing[[
            'Symbol', 'Sector', 'Industry', 'ListingDate', 'SettleMonth', 'Representative', 'HomePage', 'Region'
        ]]

        df = pd.merge(df_left, df_right, how='left', left_on='Symbol', right_on='Symbol')
        summary_category = {'시가총액': '시가총액', 'PER': 'PER', 'PBR': 'PBR', 'FCF': '잉여현금흐름'}
        # 차입금: 단기 차입금 + 장기 차입금 + 비유동금융부채 + 사채 + 유동성 장기부채

        profit_and_loss_category = {'매출액(수익)': '매출액', '매출총이익': '매출총이익', '영업이익': '영업이익', '당기순이익': '순이익'}
        financial_position_category = {
            '자산총계': '총자산',
            '단기차입금': None,
            '장기차입금': None,
            '비유동금융부채': None,
            '사채': None,
            '유동성장기부채': None
        }
        cash_flow_category = {'영업활동으로인한현금흐름': '영업활동현금흐름'}
        categories_group = [summary_category, profit_and_loss_category, financial_position_category, cash_flow_category]
        ren_cols = {
            'Symbol': 'Symbol',
            'Market': 'Market',
            'Name': 'Name',
            'Sector': 'Sector',
            'Industry': 'Industry',
            'ListingDate': 'ListingDate',
        }
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
            if pd.isna(target['ListingDate']).any():
                continue
            code = str(target['Symbol'].values[0])

            summary_url = f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}'
            financial_analysis_url = f'https://navercomp.wisereport.co.kr/v2/company/c1030001.aspx?cmp_cd={code}&cn='
            urls = [[summary_url, summary_category.keys()], [financial_analysis_url,
                                                             profit_and_loss_category.keys()],
                    [financial_analysis_url, financial_position_category.keys()],
                    [financial_analysis_url, cash_flow_category.keys()]]

            for url_index, (target_url, target_categories) in enumerate(urls):
                # scraper = cloudscraper.create_scraper(
                #     browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False})
                # ratios_html = scraper.get(target_url).content
                # ratios_soup = BeautifulSoup(ratios_html, 'lxml')
                options = webdriver.ChromeOptions()
                options.add_argument('headless')  # 웹 브라우저를 띄우지 않는 headless chrome 옵션 적용
                options.add_argument('disable-gpu')  # GPU 사용 안함
                options.add_argument('lang=ko_KR')  # 언어 설정
                driver = webdriver.Chrome(executable_path="/Users/user/PycharmProjects/auto_trading/chromedriver",
                                          options=options)
                driver.get(target_url)
                if url_index == 0:
                    try:
                        elem = driver.find_element(By.ID, 'cns_Tab22')
                        elem.click()
                    except:
                        pass
                else:
                    try:
                        elem = driver.find_element(By.ID, 'frqTyp1')
                        elem.click()
                        elem = driver.find_element(By.ID, 'hfinGubun')
                        elem.click()  # rpt_tab2
                        if url_index == 1:
                            elem = driver.find_element(By.ID, 'rpt_tab1')
                        elif url_index == 2:
                            elem = driver.find_element(By.ID, 'rpt_tab2')
                        elif url_index == 3:
                            elem = driver.find_element(By.ID, 'rpt_tab3')
                        elem.click()  #
                    except:
                        pass
                ratios_soup = BeautifulSoup(driver.page_source, 'html.parser')
                if url_index == 0:
                    tables1 = ratios_soup.find_all('table', {"class": "gHead"})
                    tables2 = ratios_soup.find_all('table', {"class": "gHead01 all-width"})
                    tables = tables1 + tables2
                else:
                    tables = ratios_soup.find_all('table', {"class": "gHead01 all-width data-list"})
                for _, table in enumerate(tables):
                    trs = table.tbody.find_all('tr')
                    for tr in trs:
                        try:
                            if url_index == 0:
                                category = str(tr.find('th').text).strip()
                                if '(배)' in category:
                                    category = category.replace('(배)', '').strip()
                            else:
                                spans = tr.find('td').find_all('span')
                                for span in spans:
                                    category = span.text.strip()
                                    if category is not None:
                                        if '펼치기' in category:
                                            category = category.replace('펼치기', '').strip()
                                        break
                            for target_category in target_categories:
                                if target_category == category:
                                    tds = tr.find_all('td')
                                    if url_index == 0:
                                        if target_category == '시가총액':
                                            text = str(tds[0].text)
                                            text = float(re.sub(r'[^0-9]', '', text))
                                            df.loc[idx, f'{target_category}_0'] = float(str(text).replace(",", ''))
                                        else:
                                            try:
                                                a_0 = float(str(tds[4].text).replace(",", ''))
                                            except:
                                                a_0 = 0.
                                            try:
                                                a_1 = float(str(tds[3].text).replace(",", ''))
                                            except:
                                                a_1 = 0.
                                            try:
                                                a_4 = float(str(tds[0].text).replace(",", ''))
                                            except:
                                                a_4 = 0.
                                            df.loc[idx, f'{target_category}_0'] = a_0
                                            df.loc[idx, f'{target_category}_1'] = a_1
                                            df.loc[idx, f'{target_category}_4'] = a_4
                                    else:
                                        try:
                                            a_0 = float(str(tds[5].text).replace(",", ''))
                                        except:
                                            a_0 = 0.
                                        try:
                                            a_1 = float(str(tds[4].text).replace(",", ''))
                                        except:
                                            a_1 = 0.
                                        try:
                                            a_4 = float(str(tds[1].text).replace(",", ''))
                                        except:
                                            a_4 = 0.
                                        df.loc[idx, f'{target_category}_0'] = a_0
                                        df.loc[idx, f'{target_category}_1'] = a_1
                                        df.loc[idx, f'{target_category}_4'] = a_4
                        except:
                            pass
        eps = 0.00000001
        df['GP/A_0'] = (df['매출총이익_0'] / (df['자산총계_0']) + eps).astype(float)
        df['GP/A_1'] = (df['매출총이익_1'] / (df['자산총계_1']) + eps).astype(float)
        df['GP/A_4'] = (df['매출총이익_4'] / (df['자산총계_4']) + eps).astype(float)
        ren_cols['GP/A_0'] = 'GP/A_0'

        df['자산성장률(년)'] = (df['자산총계_0'] / (df['자산총계_4'] + eps) - 1.).astype(float)
        ren_cols['자산성장률(년)'] = '자산성장률(년)'

        df['영업이익성장률(분기)'] = (df['영업이익_0'] / (df['영업이익_1'] + eps) - 1.).astype(float)
        ren_cols['영업이익성장률(분기)'] = '영업이익성장률(분기)'
        df['영업이익성장률(년)'] = (df['영업이익_0'] / (df['영업이익_4'] + eps) - 1.).astype(float)
        ren_cols['영업이익성장률(년)'] = '영업이익성장률(년)'

        df['순이익성장률(분기)'] = (df['당기순이익_0'] / (df['당기순이익_1'] + eps) - 1.).astype(float)
        ren_cols['순이익성장률(분기)'] = '순이익성장률(분기)'
        df['순이익성장률(년)'] = (df['당기순이익_0'] / (df['당기순이익_4'] + eps) - 1.).astype(float)
        ren_cols['순이익성장률(년)'] = '순이익성장률(년)'

        #         # 차입금: 단기차입금 + 장기차입금 + 비유동금융부채 + 사채 + 유동성장기부채
        df['차입금_0'] = (df['단기차입금_0'] + df['장기차입금_0'] + df['비유동금융부채_0'] + df['사채_0'] + df['유동성장기부채_0']).astype(float)
        df['차입금_1'] = (df['단기차입금_1'] + df['장기차입금_1'] + df['비유동금융부채_1'] + df['사채_1'] + df['유동성장기부채_1']).astype(float)
        df['차입금_4'] = (df['단기차입금_4'] + df['장기차입금_4'] + df['비유동금융부채_4'] + df['사채_4'] + df['유동성장기부채_4']).astype(float)

        ren_cols['차입금_0'] = '차입금_0'

        a_0 = (df['영업이익_0'] / (df['차입금_0']) + eps).astype(float)
        a_1 = (df['영업이익_1'] / (df['차입금_1']) + eps).astype(float)
        df['(영업이익/차입금)증가율(분기)'] = (a_0 / (a_1 + eps) - 1.).astype(float)
        ren_cols['(영업이익/차입금)증가율(분기)'] = '(영업이익/차입금)증가율(분기)'

        df['차입금증가율(년)'] = (df['차입금_0'] / (df['차입금_1'] + eps) - 1.).astype(float)
        ren_cols['차입금증가율(년)'] = '차입금증가율(년)'
        df['PSR_0'] = (df['시가총액_0'] / (df['매출액(수익)_0'] + eps)).astype(float)
        ren_cols['PSR_0'] = 'PSR_0'
        df['PFCR_0'] = (df['시가총액_0'] / (df['FCF_0'] + eps)).astype(float)
        ren_cols['PFCR_0'] = 'PFCR_0'
        df = df[ren_cols.keys()]
        df.rename(columns=ren_cols, inplace=True)
        df.reset_index(drop=True, inplace=True)
        df = df.round(3)
        if self.market in ['KONEX', 'KOSDAQ', 'KOSPI']:
            return df[df['Market'] == self.market]
        return df


class KrxDelisting:
    def __init__(self, market):
        self.market = market

    def read(self):
        data = {
            'bld': 'dbms/MDC/STAT/issue/MDCSTAT23801',
            'mktId': 'ALL',
            'isuCd': 'ALL',
            'isuCd2': 'ALL',
            'strtDd': '19900101',
            'endDd': '22001231',
            'share': '1',
            'csvxls_isNo': 'true',
        }

        headers = {
            'User-Agent': 'Chrome/78.0.3904.87 Safari/537.36',
        }

        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        r = requests.post(url, data, headers=headers)
        j = json.loads(r.text)
        df = pd.json_normalize(j['output'])
        col_map = {
            'ISU_CD': 'Symbol',
            'ISU_NM': 'Name',
            'MKT_NM': 'Market',
            'SECUGRP_NM': 'SecuGroup',
            'KIND_STKCERT_TP_NM': 'Kind',
            'LIST_DD': 'ListingDate',
            'DELIST_DD': 'DelistingDate',
            'DELIST_RSN_DSC': 'Reason',
            'ARRANTRD_MKTACT_ENFORCE_DD': 'ArrantEnforceDate',
            'ARRANTRD_END_DD': 'ArrantEndDate',
            'IDX_IND_NM': 'Industry',
            'PARVAL': 'ParValue',
            'LIST_SHRS': 'ListingShares',
            'TO_ISU_SRT_CD': 'ToSymbol',
            'TO_ISU_ABBRV': 'ToName'
        }

        df = df.rename(columns=col_map)
        df['ListingDate'] = pd.to_datetime(df['ListingDate'], format='%Y/%m/%d')
        df['DelistingDate'] = pd.to_datetime(df['DelistingDate'], format='%Y/%m/%d')
        df['ArrantEnforceDate'] = pd.to_datetime(df['ArrantEnforceDate'], format='%Y/%m/%d', errors='coerce')
        df['ArrantEndDate'] = pd.to_datetime(df['ArrantEndDate'], format='%Y/%m/%d', errors='coerce')
        df['ParValue'] = pd.to_numeric(df['ParValue'].str.replace(',', ''), errors='coerce')
        df['ListingShares'] = pd.to_numeric(df['ListingShares'].str.replace(',', ''), errors='coerce')
        return df


class KrxMarcapListing:
    def __init__(self, market):
        self.market = market

    def read(self):
        url = 'http://data.krx.co.kr/comm/bldAttendant/executeForResourceBundle.cmd?baseName=krx.mdc.i18n.component&key=B128.bld'
        j = json.loads(requests.get(url).text)
        date_str = j['result']['output'][0]['max_work_dt']

        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
        data = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01501',
            'mktId': 'ALL',
            'trdDd': date_str,
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false',
        }
        j = json.loads(requests.post(url, data).text)
        df = pd.json_normalize(j['OutBlock_1'])
        df = df.replace(',', '', regex=True)
        numeric_cols = [
            'CMPPREVDD_PRC', 'FLUC_RT', 'TDD_OPNPRC', 'TDD_HGPRC', 'TDD_LWPRC', 'ACC_TRDVOL', 'ACC_TRDVAL', 'MKTCAP',
            'LIST_SHRS'
        ]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

        df = df.sort_values('MKTCAP', ascending=False)
        cols_map = {
            'ISU_SRT_CD': 'Code',
            'ISU_ABBRV': 'Name',
            'TDD_CLSPRC': 'Close',
            'SECT_TP_NM': 'Dept',
            'FLUC_TP_CD': 'ChangeCode',
            'CMPPREVDD_PRC': 'Changes',
            'FLUC_RT': 'ChagesRatio',
            'ACC_TRDVOL': 'Volume',
            'ACC_TRDVAL': 'Amount',
            'TDD_OPNPRC': 'Open',
            'TDD_HGPRC': 'High',
            'TDD_LWPRC': 'Low',
            'MKTCAP': 'Marcap',
            'LIST_SHRS': 'Stocks',
            'MKT_NM': 'Market',
            'MKT_ID': 'MarketId'
        }
        df = df.rename(columns=cols_map)
        df.index = np.arange(len(df)) + 1
        return df


class KrxAdministrative:
    def __init__(self, market):
        self.market = market

    def read(self):
        url = "http://kind.krx.co.kr/investwarn/adminissue.do?method=searchAdminIssueSub&currentPageSize=5000&forward=adminissue_down"
        df = pd.read_html(url, header=0)[0]
        df['종목코드'] = df['종목코드'].apply(lambda x: '{:0>6d}'.format(x))
        df['지정일'] = pd.to_datetime(df['지정일'])
        col_map = {'종목코드': 'Symbol', '종목명': 'Name', '지정일': 'DesignationDate', '지정사유': 'Reason'}
        df.rename(columns=col_map, inplace=True)
        return df[['Symbol', 'Name', 'DesignationDate', 'Reason']]
