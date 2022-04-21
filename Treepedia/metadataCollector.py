"""
This function is used to collect the metadata of the GSV
panoramas based on the sample point shapefile

Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
First version July 21 2017
"""


import argparse
import os
import os.path
import time
from pathlib import Path
from urllib.request import urlopen

import xmltodict
from osgeo import ogr, osr
from tqdm.auto import tqdm


def GSVpanoMetadataCollector(
    samplesFeatureClass: Path, outputTextFolder: Path, num: int
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
                lon = geom.GetX()
                lat = geom.GetY()
                key = r''  # Input Your Key here

                # get the meta data of panoramas
                urlAddress = r'http://maps.google.com/cbk?output=xml&ll=%s,%s' % (
                    lat,
                    lon,
                )

                time.sleep(0.05)
                # the output result of the meta data is a xml object
                metaDataxml = urlopen(urlAddress)
                metaData = metaDataxml.read()
                data = xmltodict.parse(metaData)

                # in case there is not panorama in the site, therefore, continue
                if data['panorama'] is None:
                    continue
                else:
                    panoInfo = data['panorama']['data_properties']

                    # get the meta data of the panorama
                    panoDate = panoInfo.items()[4][1]
                    panoId = panoInfo.items()[5][1]
                    panoLat = panoInfo.items()[8][1]
                    panoLon = panoInfo.items()[9][1]

                    print(
                        'The coordinate (%s,%s), panoId is: %s, panoDate is: %s'
                        % (panoLon, panoLat, panoId, panoDate)
                    )
                    lineTxt = 'panoID: %s panoDate: %s longitude: %s latitude: %s\n' % (
                        panoId,
                        panoDate,
                        panoLon,
                        panoLat,
                    )
                    panoInfoText.write(lineTxt)

        panoInfoText.close()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_metadata', type=Path)
    parser.add_argument('--num', type=int, default=1000)

    args = parser.parse_args()

    GSVpanoMetadataCollector(args.input_shapefile, args.output_metadata, args.num)
