import asyncio
import json
import os
from io import BytesIO
from pathlib import Path

import aiohttp
from PIL import Image
from tqdm.auto import tqdm

try:
    from utils.concurrent import gather_with_concurrency
except ModuleNotFoundError:
    from .utils.concurrent import gather_with_concurrency


async def streetview_image(
    session: aiohttp.ClientSession,
    pano_id: str,
    fov: int,
    heading: int,
    pitch: int,
    api_key: str
):
    """
    Fetches image from Stree View API

    Parameters
    ----------
    session : aiohttp.ClientSession
        async session
    pano_id : str
        id of the streetview panorama
    fov : int
        pano field of view
    heading : int
        pano heading
    pitch : int
        pano pitch
    api_key : str
        the Google Street View API key
    """
    async with session.get(
        'http://maps.googleapis.com/maps/api/streetview',
        params={
            'size': '400x400',
            'pano': pano_id,
            'fov': fov,
            'heading': heading,
            'pitch': pitch,
            'sensor': 'false',
            'key': api_key
        }
    ) as response:
        info = (pano_id, fov, heading, pitch)
        if response.ok:
            img_content = await response.content.read()
            return info, img_content
        return info, None


async def pano_images_collector(
    input_metadata: Path,
    output_streetview: Path,
    greenmonths: list,
    num_gsv_imgs: int,
    api_key: str,
):
    """
    This function is used to download the GSV from the information provide
    by the gsv info txt, and save the result to a shapefile

    Parameters
    ----------
    input_metadata : Path
        the input folder name of GSV info
    output_greenview : Path
        the output folder to store result green result
    greenmonths : list
        a list of the green season,
        for example in Boston, greenmonth = ['05','06','07','08','09']
    num_gsv_imgs : int
        the number of images to view at each point
    api_key : str
        the Google Street View API key
    """
    fov = 360 // num_gsv_imgs
    pitch = 0

    headings = [int(fov * i) for i in range(3)]

    output_streetview.mkdir(exist_ok=True)

    metadata_files = input_metadata.glob('*.jsonl')

    async with aiohttp.TCPConnector(
        limit=None, ttl_dns_cache=300
    ) as conn:
        async with aiohttp.ClientSession(
            connector=conn, auto_decompress=False
        ) as session:

            viewed_pano_ids = output_streetview.glob('*')
            viewed_pano_ids = [fp.stem for fp in viewed_pano_ids]
            viewed_pano_ids = set(viewed_pano_ids)

            for meta_file in tqdm(
                sorted(metadata_files), desc='fetching street view images'
            ):
                with meta_file.open('r') as f:
                    lines = f.readlines()

                # create empty lists, to store the information of panos,
                # and remove duplicates
                pano_ids = []
                pano_dates = []
                pano_longitudes = []
                pano_latitudes = []

                # loop all lines in the txt files
                for line in tqdm(lines, leave=False):
                    metadata = json.loads(line)

                    pano_id = metadata['panoID']
                    pano_date = metadata['panoDate']
                    month = pano_date.split('-')[-1]
                    lng = metadata['longitude']
                    lat = metadata['latitude']

                    if (
                        int(month) not in map(int, greenmonths)
                        or pano_id in viewed_pano_ids
                    ):
                        continue
                    else:
                        viewed_pano_ids.add(pano_id)
                        pano_ids.append(pano_id)

                svi_requests = []
                for pano_id, in pano_ids:
                    for heading in headings:
                        svi_requests.append(
                            streetview_image(
                                session, pano_id, fov, heading, pitch, api_key
                            )
                        )

                batch_images = await gather_with_concurrency(*svi_requests)

                for info, image_content in batch_images:
                    if image_content is not None:
                        pano_id, fov, heading, pitch = info
                        img = Image.open(BytesIO(image_content))
                        pano_streetview_images = output_streetview / pano_id
                        pano_streetview_images.mkdir(exist_ok=True)
                        img_name = f'{fov}-{heading}-{pitch}.png'
                        img.save(pano_streetview_images / img_name)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_metadata', type=Path)
    parser.add_argument('output_greenview', type=Path)
    parser.add_argument(
        '--greenmonth',
        type=list,
        default=list(range(1, 13))
    )
    parser.add_argument(
        '--num_images',
        type=int,
        default=3
    )

    args = parser.parse_args()

    API_KEY = os.getenv('MAPS_KEY')
    if API_KEY is None:
        raise Exception('MAPS_KEY not set')

    asyncio.run(
        pano_images_collector(
            args.input_metadata,
            args.output_greenview,
            args.greenmonth,
            args.num_images,
            API_KEY,
        )
    )
