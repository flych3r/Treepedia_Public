"""
This function is used to collect the metadata of the GSV
panoramas based on the sample point shapefile

Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
First version July 21 2017
"""


import argparse
import json
import os
import os.path
import time
from pathlib import Path

import requests
from osgeo import ogr, osr
from tqdm.auto import tqdm


def GSVpanoMetadataCollector(
    samplesFeatureClass: Path, outputTextFolder: Path, key: str, num: int
):
    """
    This function is used to call the Google API url to collect the metadata of
    Google Street View Panoramas. The input of the function is the shapefile of
    the create sample site, the output is the generate metrics stored in the text file

    Parameters
    __________
    samplesFeatureClass: Path
        the shapefile of the create sample sites
    ouputTextFolder: Path
        the output folder for the metrics
    key: str
        Google Street View API Key
    num: int
        the number of sites processed every time
    """

    if not os.path.exists(outputTextFolder):
        os.makedirs(outputTextFolder)

    driver = ogr.GetDriverByName('ESRI Shapefile')

    # change the projection of shapefile to the WGS84
    dataset = driver.Open(str(samplesFeatureClass))
    layer = dataset.GetLayer()

    sourceProj = layer.GetSpatialRef()
    targetProj = osr.SpatialReference()
    targetProj.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(sourceProj, targetProj)

    # loop all the features in the featureclass
    feature = layer.GetNextFeature()
    featureNum = layer.GetFeatureCount()
    num = min(num, featureNum)
    batch = featureNum // num
    for b in tqdm(range(batch)):
        # for each batch process num GSV site
        start = b * num
        end = (b + 1) * num
        if end > featureNum:
            end = featureNum

        outputTextFile = 'Pnt_start%s_end%s.txt' % (start, end)
        outputGSVinfoFile = os.path.join(outputTextFolder, outputTextFile)

        # skip over those existing txt files
        if os.path.exists(outputGSVinfoFile):
            continue

        time.sleep(1)

        with open(outputGSVinfoFile, 'w') as panoInfoText:
            # process num feature each time
            for i in tqdm(range(start, end), leave=False):
                feature = layer.GetFeature(i)
                geom = feature.GetGeometryRef()

                # transform the current projection of input shapefile to WGS84
                # WGS84 is Earth centered, earth fixed terrestrial ref system
                geom.Transform(transform)
                lat = geom.GetX()
                lon = geom.GetY()

                time.sleep(0.05)
                metadata = requests.get(
                    'https://maps.googleapis.com/maps/api/streetview/metadata',
                    params={
                        'location': f'{lat},{lon}',
                        'key': key
                    }
                )
                metadata_json = metadata.json()

                # in case there is not panorama in the site, therefore, continue
                if metadata_json['status'] != 'OK':
                    continue
                else:
                    # get the meta data of the panorama
                    panoDate = metadata_json['date']
                    panoId = metadata_json['pano_id']
                    panoLat = metadata_json['location']['lat']
                    panoLng = metadata_json['location']['lng']

                    jsonLine = {
                        'panoID': panoId,
                        'panoDate': panoDate,
                        'longitude': panoLng,
                        'latitude': panoLat
                    }
                    panoInfoText.write(f'{json.dumps(jsonLine)}\n')

        panoInfoText.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_metadata', type=Path)
    parser.add_argument('--num', type=int, default=1000)

    key = os.getenv('MAPS_KEY')
    if key is None:
        raise Exception('MAPS_KEY not set')

    args = parser.parse_args()

    GSVpanoMetadataCollector(
        args.input_shapefile, args.output_metadata, key, args.num
    )
