"""
This function is used to collect the metadata of the GSV
panoramas based on the sample point shapefile

Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
First version July 21 2017
"""

import json
import os
from math import ceil
from pathlib import Path

import requests
from osgeo import ogr, osr
from tqdm.auto import tqdm


def pano_metadata_collector(
    input_shapefile: Path, output_metadata: Path, num_sites: int, api_key: str
):
    """
    This function is used to call the Google API url to collect the metadata of
    Google Street View Panoramas. The input of the function is the shapefile of
    the create sample site, the output is the generate metrics stored in the text file

    Parameters
    ----------
    input_shapefile: Path
        the shapefile of the create sample sites
    output_metadata: Path
        the output folder for the metrics
    num_sites: int
        the number of sites processed every time
    api_key: str
        Google Street View API Key
    """
    output_metadata.mkdir(exist_ok=True)

    driver = ogr.GetDriverByName('ESRI Shapefile')

    # change the projection of shapefile to the WGS84
    dataset = driver.Open(str(input_shapefile))
    layer = dataset.GetLayer()

    source_proj = layer.GetSpatialRef()
    target_proj = osr.SpatialReference()
    target_proj.ImportFromEPSG(4326)
    transform = osr.CoordinateTransformation(source_proj, target_proj)

    # loop all the features in the featureclass
    feature = layer.GetNextFeature()
    num_features = layer.GetFeatureCount()
    num_sites = min(num_sites, num_features)
    batch = ceil(num_features / num_sites)
    unique_pano_ids = set()

    for b in tqdm(range(batch), desc='fetching gsv metadata'):
        # for each batch process num GSV site
        start = b * num_sites
        end = (b + 1) * num_sites
        if end > num_features:
            end = num_features

        output_file_name = f'Pnt_start{start}_end{end}.jsonl'
        output_file_path = output_metadata / output_file_name

        # skip over those existing txt files
        if output_file_path.exists():
            continue

        with output_file_path.open('w') as pano_metadata:
            # process num feature each time
            for i in tqdm(range(start, end), leave=False):
                feature = layer.GetFeature(i)
                geom = feature.GetGeometryRef()

                # transform the current projection of input shapefile to WGS84
                # WGS84 is Earth centered, earth fixed terrestrial ref system
                geom.Transform(transform)
                lat = geom.GetX()
                lon = geom.GetY()

                metadata = requests.get(
                    'https://maps.googleapis.com/maps/api/streetview/metadata',
                    params={
                        'location': f'{lat},{lon}',
                        'key': api_key
                    }
                )
                metadata_json = metadata.json()

                # in case there is not panorama in the site, therefore, continue
                if metadata_json['status'] != 'OK':
                    continue

                # get the meta data of the panorama
                pano_date = metadata_json['date']
                pano_id = metadata_json['pano_id']
                pano_lat = metadata_json['location']['lat']
                pano_lng = metadata_json['location']['lng']

                if pano_id in unique_pano_ids:
                    continue

                unique_pano_ids.add(pano_id)
                json_line = {
                    'panoID': pano_id,
                    'panoDate': pano_date,
                    'longitude': pano_lng,
                    'latitude': pano_lat
                }
                pano_metadata.write(f'{json.dumps(json_line)}\n')


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_metadata', type=Path)
    parser.add_argument('--num', type=int, default=1000)

    API_KEY = os.getenv('MAPS_KEY')
    if API_KEY is None:
        raise Exception('MAPS_KEY not set')

    args = parser.parse_args()

    pano_metadata_collector(
        args.input_shapefile, args.output_metadata, args.num, API_KEY
    )
