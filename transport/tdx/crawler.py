import pandas as pd
import json
import requests
from datetime import datetime


class Crawler:
    def __init__(self) -> None:
        self.api_url = 'https://tdx.transportdata.tw/api/'
        self.auth_url = 'https://tdx.transportdata.tw/auth/realms/TDXConnect/protocol/openid-connect/token'
        
    def get_token(self, client_id: str, client_secret: str) -> str:
        auth_response = requests.post(
            self.auth_url,
            {
                'client_id': client_id,
                'client_secret': client_secret,
                'content_type': 'application/x-www-form-urlencoded',
                'grant_type': 'client_credentials'
            }
        )
        return json.loads(auth_response.text)['access_token']

    def response(self,
                 client_id: str,
                 client_secret: str,
                 target: str,
                 fileformat: str,
                 date: str = None,
                 top: int = None,
                 skip: int = None,
                 authority: str = None,
                 city: str = None,
                 rail_operator: str = None,
                 airport: str = None,
                 link_id: any = None,
                 method: str = 'get') -> requests.models.Response:
        """ Raise request to TDX API Server & return response """
        argcount = 0
        if (city):
            target += f"/City/{city}?"
        elif (authority):
            target += f"/{authority}?"
        elif (rail_operator):
            target += f"/Rail/{rail_operator}?"
        elif (airport):
            target += f"/Air/Airport/{airport}?"
        elif (link_id):
            if (type(link_id) == str):
                target += f"/{link_id}?"
            elif (type(link_id) == list):
                target += '?'
                method = 'post'

        if (top):
            target += f"%24top={top}"
            argcount += 1
        
        if (skip):
            target += f"&%24skip={skip}"
            argcount += 1

        # For crawling historical data
        if (date):
            target += f"Dates={date}"
            argcount += 1
        
        # Required
        if (argcount > 0):
            target += f"&%24format={fileformat}"
        else:
            target += f"%24format={fileformat}"

        if (method == 'get'):
            try:
                api_key = f"Bearer {self.get_token(client_id, client_secret)}"
                headers = {
                    'Authorization': api_key
                }
                rtn = requests.get(url=f"{self.api_url}{target}", headers=headers)
            except:
                raise RuntimeError('There might be something wrong according to your input args. Check all your input args to ensure the API server return successfully.')
        
        elif (method == 'post'):
            try:
                api_key = f"Bearer {self.get_token(client_id, client_secret)}"
                headers = {
                    'Authorization': api_key
                }
                rtn = requests.post(url=f"{self.api_url}{target}", headers=headers, json=link_id)
            except:
                raise RuntimeError('There might be something wrong according to your input args. Check all your input args to ensure the API server return successfully.')
            
        return rtn

    def download(self, content: requests.models.Response,
                 filedir: str,
                 date: str,
                 filename: str,
                 fileformat: str) -> None:
        with open(f"{filedir}/{filename}{date}.{fileformat}", 'w') as f:
            f.write(content.text)
            f.close()


class BasicDataCrawler(Crawler):
    def __init__(self) -> None:
        super().__init__()
        self.api_url = 'https://tdx.transportdata.tw/api/basic'


class HistDataCrawler(Crawler):
    def __init__(self) -> None:
        super().__init__()
        self.api_url = 'https://tdx.transportdata.tw/api/historical'


class RealTimeRoadInfoCrawler(BasicDataCrawler):
    def __init__(self) -> None:
        super().__init__()
        self.api_url += '/v2/Road/Traffic'


class LinkInfoCrawler(BasicDataCrawler):
    def __init__(self) -> None:
        super().__init__()
        self.api_url += '/v2/Road/Link'


class HistRoadInfoCrawler(HistDataCrawler):
    def __init__(self) -> None:
        super().__init__()
        self.api_url += '/v2/Historical/Road/Traffic'


if __name__ == '__main__':
    filedir = './trafficData'
    dateStart = '2024-04-19'
    dateEnd = '2024-04-19'
    dateList = list(map(lambda x: datetime.strftime(x, '%Y-%m-%d'), pd.date_range(dateStart, dateEnd)))[::-1]

    crawler = HistRoadInfoCrawler()

    # Dates: 'YYYY-mm-dd' or 'YYYY-mm-dd~YYYY-mm-dd' (At most 7 days)
    for date in dateList:
        print(f"============================ {date} ============================")
        print('... VD 歷史資料 ...')
        # VD 歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Kaohsiung / Keelung / YilanCounty / TaitungCounty
        rtn = crawler.response(target='/VD', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市車輛偵測器歷史資料')
        
        print('... VD 即時路況歷史資料 ...')
        # VD 即時路況歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Kaohsiung / Keelung / YilanCounty / TaitungCounty
        rtn = crawler.response(target='/Live/VD', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市車輛偵測器即時路況歷史資料')

        print('... 發佈路段歷史資料 ...')
        # 發佈路段歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Keelung / ChanghuaCounty / YilanCounty
        rtn = crawler.response(target='/Section', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市發佈路段歷史資料')

        print('... 發佈路段即時路況歷史資料 ...')
        # 發佈路段即時路況歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Keelung / ChanghuaCounty / YilanCounty
        rtn = crawler.response(target='/Live', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市發佈路段即時路況歷史資料')

        print('... 發佈路段之基礎路段組合歷史資料 ...')
        # 發佈路段之基礎路段組合歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Keelung / ChanghuaCounty / YilanCounty
        rtn = crawler.response(target='/SectionLink', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市發佈路段之基礎路段組合歷史資料')
        
        print('... 發佈路段線型圖資歷史資料 ...')
        # 發佈路段線型圖資歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Keelung / ChanghuaCounty / YilanCounty
        rtn = crawler.response(target='/SectionShape', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市發佈路段線型圖資歷史資料')
        
        print('... 路況壅塞水準歷史資料 ...')
        # 路況壅塞水準歷史資料
        # City: Taipei / NewTaipei / Taoyuan / Taichung / Tainan / Kaohsiung / Keelung / ChanghuaCounty / YunlinCounty / PingtungCounty / YilanCounty
        rtn = crawler.response(target='/CongestionLevel', date=date, city='Taipei')
        crawler.download(rtn, filedir, date, filename='臺北市路況壅塞水準歷史資料')
