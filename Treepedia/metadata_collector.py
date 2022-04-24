"""
This function is used to collect the metadata of the GSV
panoramas based on the sample point shapefile

Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
First version July 21 2017
"""

import asyncio
import json
import os
from math import ceil
from pathlib import Path

import aiohttp
from osgeo import ogr, osr
from tqdm.auto import tqdm
from yarl import URL

try:
    from utils.concurrent import gather_with_concurrency
    from utils.url import add_params_to_url, sign_url
except ModuleNotFoundError:
    from .utils.concurrent import gather_with_concurrency
    from .utils.url import add_params_to_url, sign_url


async def streetview_metadata(
    session: aiohttp.ClientSession,
    lat: float,
    lon: float,
    api_key: str,
    signature_secret: str
) -> dict:
    """
    Fetches street view metadata asynchronously

    Parameters
    ----------
    session : aiohttp.ClientSession
        client session
    lat : float
        point latitude
    lon : float
        point longitude
    api_key : str
         Google Street View API Key
    signature_secret: str
        Google Street View Signature Key

    Returns
    -------
    dict
        json response
    """
    request_url = add_params_to_url(
        'https://maps.googleapis.com/maps/api/streetview/metadata',
        params={
            'location': f'{lat},{lon}',
            'key': api_key
        }
    )
    signed_url = sign_url(request_url, signature_secret)
    async with session.get(URL(signed_url, encoded=True)) as response:
        return await response.json()


async def pano_metadata_collector(
    input_shapefile: Path,
    output_metadata: Path,
    num_sites: int,
    api_key: str,
    signature_secret: str
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
    signature_secret: str
        Google Street View Signature Key
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

    async with aiohttp.TCPConnector(limit=None, ttl_dns_cache=300) as conn:
        async with aiohttp.ClientSession(connector=conn) as session:

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
                    latitudes, longitudes = [], []
                    for i in tqdm(range(start, end), leave=False):
                        feature = layer.GetFeature(i)
                        geom = feature.GetGeometryRef()

                        # transform the current projection of input shapefile to WGS84
                        # WGS84 is Earth centered, earth fixed terrestrial ref system
                        geom.Transform(transform)
                        latitudes.append(geom.GetX())
                        longitudes.append(geom.GetY())

                    batch_metadata = await gather_with_concurrency(*[
                        streetview_metadata(session, lat, lon, api_key, signature_secret)
                        for lat, lon in zip(latitudes, longitudes)
                    ])

                    for metadata_json in batch_metadata:
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
    parser.add_argument('--num', type=int, default=500)

    API_KEY = os.getenv('MAPS_KEY')
    SIGNATURE_SECRET = os.getenv('SIGNATURE_SECRET')
    if API_KEY is None or SIGNATURE_SECRET is None:
        raise Exception('MAPS_KEY or SIGNATURE_SECRET not set')

    args = parser.parse_args()

    asyncio.run(
        pano_metadata_collector(
            args.input_shapefile,
            args.output_metadata,
            args.num,
            API_KEY,
            SIGNATURE_SECRET
        )
    )
