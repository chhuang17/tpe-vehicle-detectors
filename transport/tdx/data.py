import polars as pl
import pandas as pd
import json
from datetime import datetime
from .crawler import HistRoadInfoCrawler, RealTimeRoadInfoCrawler, LinkInfoCrawler


link_crawler = LinkInfoCrawler()

def getLinkInfo(contents: list, df_type: str = 'polars') -> any:
    # linkInfo = link_crawler.response(target='/LinkID', link_id=link_id, fileformat='JSON')
    if (df_type == 'polars'):
        # return pl.DataFrame(json.loads(linkInfo.text))
        return pl.DataFrame(contents)
    elif (df_type == 'pandas'):
        # return pd.DataFrame(json.loads(linkInfo.text))
        return pd.DataFrame(contents)
    elif (df_type == 'dict'):
        # return json.loads(linkInfo.text)
        return contents
    else:
        raise ValueError(f"'{df_type}' is not defined.")

def getVDStatic(contents: list, date: str, df_type: str = 'polars') -> any:
    """ Get VD Static Data from TDX (daily updated) """
    vdStacInfo = []
    for content in contents:
        if (content.startswith('\ufeff')):
            content = content[1:]
        content = json.loads(content)
        if (datetime.strptime(date, '%Y-%m-%d').date() < datetime.now().date()):
            data = {}
            data['VDID'] = content['VDID']
            data['AutorityCode'] = content['AuthorityCode']
            data['BiDirectional'] = content['BiDirectional']
            for item in content['DetectionLinks']:
                data['DetectionLinkID'] = item['LinkID']
                data['Bearing'] = item['Bearing']
                data['RoadDirection'] = item['RoadDirection']
                data['LaneNum'] = item['LaneNum']
                data['ActualLaneNum'] = item['ActualLaneNum']
            data['VDType'] = content['VDType']
            data['DetectionType'] = content['DetectionType']
            data['PositionLon'] = content['PositionLon']
            data['PositionLat'] = content['PositionLat']
            data['CountyName'] = content['CountyName']
            data['TownName'] = content['TownName']
            data['RoadID'] = content['RoadID']
            data['RoadName'] = content['RoadName']
            data['InfoTime'] = content['InfoTime'].replace('T',' ')[:-6]
            data['UpdateTime'] = content['UpdateTime'].replace('T',' ')[:-6]
            vdStacInfo.append(data)
        else:
            for vdInfo in content['VDs']:
                data = {}
                data['VDID'] = vdInfo['VDID']
                data['AutorityCode'] = content['AuthorityCode']
                data['BiDirectional'] = vdInfo['BiDirectional']
                for item in vdInfo['DetectionLinks']:
                    data['DetectionLinkID'] = item['LinkID']
                    data['Bearing'] = item['Bearing']
                    data['RoadDirection'] = item['RoadDirection']
                    data['LaneNum'] = item['LaneNum']
                    data['ActualLaneNum'] = item['ActualLaneNum']
                data['VDType'] = vdInfo['VDType']
                data['DetectionType'] = vdInfo['DetectionType']
                data['PositionLon'] = vdInfo['PositionLon']
                data['PositionLat'] = vdInfo['PositionLat']
                data['CountyName'] = None
                data['TownName'] = None
                data['RoadID'] = vdInfo['RoadID']
                data['RoadName'] = vdInfo['RoadName']
                data['InfoTime'] = content['SrcUpdateTime'].replace('T',' ')[:-6]
                data['UpdateTime'] = content['UpdateTime'].replace('T',' ')[:-6]
                vdStacInfo.append(data)

    if (df_type == 'polars'):
        return pl.DataFrame(vdStacInfo)
    elif (df_type == 'pandas'):
        return pd.DataFrame(vdStacInfo)
    elif (df_type == 'dict'):
        return vdStacInfo
    else:
        raise ValueError(f"'{df_type}' is not defined.")
    
def getVDDynamic(contents: list, date: str = None, df_type: str = 'polars') -> any:
    """ Get VD Dynamic Data from TDX (updated per minute) """
    vdDymcInfo = []
    for content in contents:
        if (content.startswith('\ufeff')):
            content = content[1:]
        content = json.loads(content)
        if (date) and (datetime.strptime(date, '%Y-%m-%d').date() < datetime.now().date()):
            data = {}
            data['VDID'] = content['VDID']
            data['AutorityCode'] = content['AuthorityCode']
            for item in content['LinkFlows']:
                data['LinkID'] = item['LinkID']
                for lane in item['Lanes']:
                    data['LaneID'] = lane['LaneID']
                    data['LaneType'] = lane['LaneType']
                    data['Speed'] = lane['Speed']
                    data['Occupancy'] = lane['Occupancy']
                    for veh in lane['Vehicles']:
                        if (veh['VehicleType'] == 'M'):
                            data['MotorVolume'] = veh['Volume']
                            data['MotorSpeed'] = veh['Speed']
                        elif (veh['VehicleType'] == 'S'):
                            data['SmallCarVolume'] = veh['Volume']
                            data['SmallCarSpeed'] = veh['Speed']
                        elif (veh['VehicleType'] == 'L'):
                            data['LargeCarVolume'] = veh['Volume']
                            data['LargeCarSpeed'] = veh['Speed']
                        elif (veh['VehicleType'] == 'T'):
                            data['TruckCarVolume'] = veh['Volume']
                            data['TruckCarSpeed'] = veh['Speed']
                    data['RecurrentTimes'] = lane['RecurrentTimes']
                    data['RecurrentZeroTimes'] = lane['RecurrentTimes']        
            data['Status'] = content['Status']
            data['DataCollectTime'] = content['DataCollectTime'].replace('T',' ')[:-6]
            data['InfoTime'] = content['InfoTime'].replace('T',' ')[:-6]
            data['UpdateTime'] = content['UpdateTime'].replace('T',' ')[:-6]
            vdDymcInfo.append(data)
        else:
            for vdInfo in content['VDLives']:
                data = {}
                data['VDID'] = vdInfo['VDID']
                data['AutorityCode'] = content['AuthorityCode']
                for item in vdInfo['LinkFlows']:
                    data['LinkID'] = item['LinkID']
                    for lane in item['Lanes']:
                        data['LaneID'] = lane['LaneID']
                        data['LaneType'] = lane['LaneType']
                        data['Speed'] = lane['Speed']
                        data['Occupancy'] = lane['Occupancy']
                        for veh in lane['Vehicles']:
                            if (veh['VehicleType'] == 'M'):
                                data['MotorVolume'] = veh['Volume']
                                data['MotorSpeed'] = veh['Speed']
                            elif (veh['VehicleType'] == 'S'):
                                data['SmallCarVolume'] = veh['Volume']
                                data['SmallCarSpeed'] = veh['Speed']
                            elif (veh['VehicleType'] == 'L'):
                                data['LargeCarVolume'] = veh['Volume']
                                data['LargeCarSpeed'] = veh['Speed']
                            elif (veh['VehicleType'] == 'T'):
                                data['TruckCarVolume'] = veh['Volume']
                                data['TruckCarSpeed'] = veh['Speed']
                        data['RecurrentTimes'] = None
                        data['RecurrentZeroTimes'] = None
                data['Status'] = vdInfo['Status']
                data['DataCollectTime'] = vdInfo['DataCollectTime'].replace('T',' ')[:-6]
                data['InfoTime'] = content['UpdateTime'].replace('T',' ')[:-6]
                data['UpdateTime'] = content['UpdateTime'].replace('T',' ')[:-6]
                vdDymcInfo.append(data)

    if (df_type == 'polars'):
        return pl.DataFrame(vdDymcInfo)
    elif (df_type == 'pandas'):
        return pd.DataFrame(vdDymcInfo)
    elif (df_type == 'dict'):
        return vdDymcInfo
    else:
        raise ValueError(f"'{df_type}' is not defined.")
