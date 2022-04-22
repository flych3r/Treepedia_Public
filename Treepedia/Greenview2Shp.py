"""
This script is used to convert the green view index results saved in txt to Shapefile
considering the facts many people are more comfortable with shapefile and GIS
Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
"""

import glob
import json
import os
import os.path
from pathlib import Path

from osgeo import ogr, osr
from tqdm.auto import tqdm


def Read_GSVinfo_Text(GVI_Res_txt: Path) -> tuple:
    '''
    This function is used to read the information in text files or folders
    the fundtion will remove the duplicate sites and only select those sites
    have GSV info in green month.

    Return:
        panoIDLst,panoDateLst,panoLonLst,panoLatLst,greenViewLst

    Pamameters:
        GVI_Res_txt: the file name of the GSV information txt file
    '''

    # empty list to save the GVI result and GSV metadata
    panoIDLst = []
    panoDateLst = []
    panoLonLst = []
    panoLatLst = []
    greenViewLst = []

    # read the green view index result txt files
    lines = open(GVI_Res_txt, 'r')
    for line in lines:
        data = json.loads(line)

        panoID = data['panoID']
        panoDate = '2022-04-22'  # data['panoDate']
        lon = data['longitude']
        lat = data['latitude']
        greenView = data['greenview']

        # remove the duplicated panorama id
        if panoID not in panoIDLst:
            panoIDLst.append(panoID)
            panoDateLst.append(panoDate)
            panoLonLst.append(lon)
            panoLatLst.append(lat)
            greenViewLst.append(greenView)

    return panoIDLst, panoDateLst, panoLonLst, panoLatLst, greenViewLst


# read the green view index files into list, the input can be file or folder
def Read_GVI_res(GVI_Res: Path) -> tuple:
    '''
    This function is used to read the information in text files or folders
    the fundtion will remove the duplicate sites and only select those sites
    have GSV info in green month.

    Return:
        panoIDLst,panoDateLst,panoLonLst,panoLatLst,greenViewLst

    Pamameters:
        GVI_Res: the file name of the GSV information text, could be folder or txt file

    last modified by Xiaojiang Li, March 27, 2018
    '''

    # empty list to save the GVI result and GSV metadata
    panoIDLst = []
    panoDateLst = []
    panoLonLst = []
    panoLatLst = []
    greenViewLst = []

    allTxtFiles = glob.glob(str(GVI_Res / '*.jsonl'))

    for txtfilename in allTxtFiles:
        # call the function to read txt file to a list
        gsv_info = Read_GSVinfo_Text(txtfilename)

        panoIDLst.extend(gsv_info[0])
        panoDateLst.extend(gsv_info[1])
        panoLonLst.extend(gsv_info[2])
        panoLatLst.extend(gsv_info[3])
        greenViewLst.extend(gsv_info[4])

    return panoIDLst, panoDateLst, panoLonLst, panoLatLst, greenViewLst


def CreatePointFeature_ogr(
    outputShapefile: Path,
    panoIDLst: list,
    panoDateLst: list,
    panoLonLst: list,
    panoLatLst: list,
    greenViewLst: list,
    layer_name: str
):
    """
    Create a shapefile based on the template of inputShapefile
    This function will delete existing outpuShapefile and create
    a new shapefile containing points with
    panoID, panoDate, and green view as respective fields.

    Parameters:
    outputShapefile: the file path of the output shapefile name
      LonLst: the longitude list
      LatLst: the latitude list
      panoIDlist: the panorama id list
      panoDateList: the panodate list
      greenViewList: the green view index result list, all these lists
      can be generated from the function of 'Read_GVI_res'

    Copyright(c) Xiaojiang Li, Senseable city lab

    last modified by Xiaojiang li, MIT Senseable City Lab on March 27, 2018

    """

    # create shapefile and add the above chosen random points to the shapfile
    driver = ogr.GetDriverByName('ESRI Shapefile')

    # create new shapefile
    if os.path.exists(outputShapefile):
        driver.DeleteDataSource(str(outputShapefile))

    data_source = driver.CreateDataSource(str(outputShapefile))
    targetSpatialRef = osr.SpatialReference()
    targetSpatialRef.ImportFromEPSG(4326)

    outLayer = data_source.CreateLayer(layer_name, targetSpatialRef, ogr.wkbPoint)

    # create a field
    idField = ogr.FieldDefn('PntNum', ogr.OFTInteger)
    panoID_Field = ogr.FieldDefn('panoID', ogr.OFTString)
    panoDate_Field = ogr.FieldDefn('panoDate', ogr.OFTString)
    greenView_Field = ogr.FieldDefn('greenView', ogr.OFTReal)
    outLayer.CreateField(idField)
    outLayer.CreateField(panoID_Field)
    outLayer.CreateField(panoDate_Field)
    outLayer.CreateField(greenView_Field)

    for idx, (panoID, panoDate, lat, lng, gvi) in tqdm(
        enumerate(zip(panoIDLst, panoDateLst, panoLonLst, panoLatLst, greenViewLst)),
        total=len(panoIDLst)
    ):
        # create point geometry
        point = ogr.Geometry(ogr.wkbPoint)

        point.AddPoint(float(lng), float(lat))

        # Create the feature and set values
        featureDefn = outLayer.GetLayerDefn()
        outFeature = ogr.Feature(featureDefn)
        outFeature.SetGeometry(point)
        outFeature.SetField('PntNum', idx)
        outFeature.SetField('panoID', panoID)
        outFeature.SetField('panoDate', panoDate)
        outFeature.SetField('greenView', float(gvi))

        outLayer.CreateFeature(outFeature)
        outFeature.Destroy()

    data_source.Destroy()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_greenview', type=Path)
    parser.add_argument('output_shapefile', type=Path)

    args = parser.parse_args()

    layer_name = 'greenView'

    panoIDlist, panoDateList, LonLst, LatLst, greenViewList = Read_GVI_res(
        args.input_greenview
    )

    CreatePointFeature_ogr(
        args.output_shapefile,
        panoIDlist,
        panoDateList,
        LonLst,
        LatLst,
        greenViewList,
        layer_name
    )

    print('Done!!!')
