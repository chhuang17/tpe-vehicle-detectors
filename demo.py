import polars as pl
import folium
import json
import argparse
from flask import Flask
from datetime import datetime
from transport.tdx.crawler import RealTimeRoadInfoCrawler, LinkInfoCrawler
from transport.tdx import data


app = Flask(__name__)


def trafficSituation(vdInfo: pl.DataFrame) -> tuple:
    totalVolume = vdInfo[['MotorVolume','SmallCarVolume','LargeCarVolume','TruckCarVolume']].sum_horizontal()[0]
    if (totalVolume > 0):
        if (vdInfo['Speed'][0] < 0) or (vdInfo['Occupancy'][0] < 0):
            color = 'gray'
            speed = 'NaN'
        else:
            w_vol = vdInfo[['MotorVolume','SmallCarVolume','LargeCarVolume','TruckCarVolume']]
            w_speed = vdInfo[['MotorSpeed','SmallCarSpeed','LargeCarSpeed','TruckCarSpeed']]
            speed = (w_vol.to_numpy() * w_speed.to_numpy()).sum() / totalVolume
            # 國道
            if (vdInfo['RoadClass'][0] == 0):
                if (speed >= 0) and (speed <= 20):
                    color = 'purple'
                elif (speed > 20) and (speed <= 40):
                    color = 'red'
                elif (speed > 40) and (speed <= 60):
                    color = 'orange'
                elif (speed > 60) and (speed <= 80):
                    color = 'yellow'
                elif (speed > 80):
                    color = 'green'
            
            # 省道快速公路
            elif (vdInfo['RoadClass'][0] == 1):
                if (speed >= 0) and (speed <= 20):
                    color = 'purple'
                elif (speed > 20) and (speed <= 40):
                    color = 'red'
                elif (speed > 40) and (speed <= 60):
                    color = 'orange'
                elif (speed > 60) and (speed <= 80):
                    color = 'yellow'
                elif (speed > 80):
                    color = 'green'
            
            # 市區快速道路
            elif (vdInfo['RoadClass'][0] == 2):
                if (speed >= 0) and (speed <= 20):
                    color = 'purple'
                elif (speed > 20) and (speed <= 40):
                    color = 'red'
                elif (speed > 40) and (speed <= 55):
                    color = 'orange'
                elif (speed > 55) and (speed <= 70):
                    color = 'yellow'
                elif (speed > 70):
                    color = 'green'
            
            # 省道一般公路
            elif (vdInfo['RoadClass'][0] == 3):
                if (speed >= 0) and (speed <= 10):
                    color = 'purple'
                elif (speed > 10) and (speed <= 15):
                    color = 'red'
                elif (speed > 15) and (speed <= 25):
                    color = 'orange'
                elif (speed > 25) and (speed <= 40):
                    color = 'yellow'
                elif (speed > 40):
                    color = 'green'
            
            # 市道、縣道
            elif (vdInfo['RoadClass'][0] == 4):
                if (speed >= 0) and (speed <= 10):
                    color = 'purple'
                elif (speed > 10) and (speed <= 15):
                    color = 'red'
                elif (speed > 15) and (speed <= 25):
                    color = 'orange'
                elif (speed > 25) and (speed <= 40):
                    color = 'yellow'
                elif (speed > 40):
                    color = 'green'
            
            # 市區一般道路
            elif (vdInfo['RoadClass'][0] == 6):
                if (speed >= 0) and (speed <= 10):
                    color = 'purple'
                elif (speed > 10) and (speed <= 15):
                    color = 'red'
                elif (speed > 15) and (speed <= 25):
                    color = 'orange'
                elif (speed > 25) and (speed <= 40):
                    color = 'yellow'
                elif (speed > 40):
                    color = 'green'
            
            speed = f"{speed:.2f}"
    
    # Volume equals zero
    elif (totalVolume == 0):
        if (vdInfo['Speed'][0] == 0) and (vdInfo['Occupancy'][0] == 0):
            color = 'green'
            # 國道
            if (vdInfo['RoadClass'][0] == 0):
                speed = f"{110:.2f}"
            
            # 省道快速公路
            elif (vdInfo['RoadClass'][0] == 1):
                speed = f"{90:.2f}"
            
            # 市區快速道路
            elif (vdInfo['RoadClass'][0] == 2):
                speed = f"{80:.2f}"
            
            # 省道一般公路
            elif (vdInfo['RoadClass'][0] == 3):
                speed = f"{65:.2f}"
            
            # 市道、縣道
            elif (vdInfo['RoadClass'][0] == 4):
                speed = f"{60:.2f}"
            
            # 市區一般道路
            elif (vdInfo['RoadClass'][0] == 6):
                speed = f"{60:.2f}"
        
        elif (vdInfo['Speed'][0] == 0) and (vdInfo['Occupancy'][0] != 0):
            color = 'red'
            speed = f"{vdInfo['Speed'][0]:.2f}"
        else:
            color = 'gray'
            speed = 'NaN'
    
    # Volume is less than zero -> Error
    else:
        color = 'gray'
        speed = 'NaN'

    return color, speed

def draw_vdMap():
    init_point = (25.056583067116616, 121.54849732195152)
    vdStc = realtime_crawler.response(client_id=client_id, client_secret=client_secret,
                                      target='/VD', city='Taipei', fileformat='JSON')
    vdStcDf = data.getVDStatic(contents=[vdStc.text],
                               date=datetime.strftime(datetime.now().date(), '%Y-%m-%d'))
    vdDyc = realtime_crawler.response(client_id=client_id, client_secret=client_secret,
                                      target='/Live/VD', city='Taipei', fileformat='JSON')
    vdDycDf = data.getVDDynamic(contents=[vdDyc.text])
    linkInfo = link_crawler.response(client_id=client_id, client_secret=client_secret,
                                     link_id=vdStcDf['DetectionLinkID'].to_list(),
                                     target='/LinkID', fileformat='JSON')
    linkInfoDf = data.getLinkInfo(contents=json.loads(linkInfo.text))
    linkInfoDf = linkInfoDf[['LinkID','RoadClass']]
    linkInfoDf = linkInfoDf.unique(subset=linkInfoDf.columns)

    vdInfo = vdStcDf.join(
        linkInfoDf,
        how='left',
        left_on='DetectionLinkID',
        right_on='LinkID'
    ).join(
        vdDycDf,
        how='left',
        on='VDID'
    )

    vdMap = folium.Map(location=init_point, zoom_start=30, tiles='CartoDB positron')
    for i in range(vdInfo.shape[0]):
        color, speed = trafficSituation(vdInfo[i])
        info  = '<div style="font-size: 18px">'
        if (vdInfo['BiDirectional'][i] == 1):
            info += f"設備編碼: {vdInfo['VDID'][i]} [雙向偵測]<br>"
        else:
            info += f"設備編碼: {vdInfo['VDID'][i]} [單向偵測]<br>"
        info += f"道路編號: {vdInfo['RoadID'][i]}<br>"
        info += f"道路名稱: {vdInfo['RoadName'][i]}<br>"
        info += f"座標: ({vdInfo['PositionLat'][i]}, {vdInfo['PositionLon'][i]})<br>"
        info += f"時間平均速度: {speed} km/h<br>"
        info += f"資料更新時間: {vdInfo['DataCollectTime'][i]}"
        info += '</div>'
        
        folium.Circle(
            location=(vdInfo['PositionLat'][i], vdInfo['PositionLon'][i]),
            color=color,
            radius=50,
            popup=folium.Popup(info, max_width=400),
            fill=True,
            fill_opacity=0.5
        ).add_to(vdMap)
    
    return vdMap


@app.route('/', methods=['GET'])
def index():
    vdMap = draw_vdMap()
    return vdMap._repr_html_()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--id', help='client_id')
    parser.add_argument('-s', '--secret', help='client_secret')
    args = parser.parse_args()
    
    client_id, client_secret = args.id, args.secret
    realtime_crawler = RealTimeRoadInfoCrawler()
    link_crawler = LinkInfoCrawler()
    
    app.run(port=54088)
