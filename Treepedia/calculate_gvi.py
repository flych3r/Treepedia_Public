"""
This program is used to calculate the green view index based on the collecte metadata.
The Object based images classification algorithm is used to classify the greenery
from the GSV imgs in this code, the meanshift algorithm implemented by pymeanshift was
used to segment image first, based on the segmented image, we further use the Otsu's
method to find threshold from ExG image to extract the greenery pixels.

For more details about the object based image classification algorithm
check: Li et al., 2016, Who lives in greener neighborhoods? the distribution
of street greenery and it association with residents'
socioeconomic conditions in Hartford, Connectictu, USA

This program implementing OTSU algorithm to chose the threshold automatically
For more details about the OTSU algorithm and python implmentation


Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
First version June 18, 2014
"""

import json
import os
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from skimage.filters import threshold_otsu
from skimage.segmentation import felzenszwalb
from tqdm.auto import tqdm


def segment_image(
    image: np.array, scale: int = 50, sigma: float = 0.5, min_size: int = 20
) -> np.array:
    """
    Segments the image and calculates the mean pixel values for each segment

    Parameters
    ----------
    image : np.array
        image to be segmented
    scale : int, optional
        Higher means larger clusters, by default 1
    sigma : float, optional
        Width (standard deviation) of Gaussian kernel
        used in preprocessing, by default 0.8
    min_size : int, optional
        Minimum component size, by default 20

    Returns
    -------
    np.array
        _description_
    """
    image_semgented = image.copy()
    segments_fz = felzenszwalb(
        image_semgented, scale=scale, sigma=sigma, min_size=min_size
    )
    for seg in np.unique(segments_fz):
        mask = segments_fz == seg
        image_semgented[mask] = image_semgented[mask].mean(axis=0)
    return image_semgented


def vegetation_classification(image: np.array, segment: bool = True) -> float:
    """
    This function is used to classify the green vegetation from GSV image,
    This is based on object based and otsu automatically thresholding method
    The season of GSV images were also considered in this function

    Parameters
    ----------
    image : np.array
        the numpy array image
    segment: bool, optional
        if the image will be segmented before calculating gvi, by default True

    Returns
    -------
    float
        the percentage of the green vegetation pixels in the GSV image

    By Xiaojiang Li
    """
    img = image / 255.0
    if segment:
        img = segment_image(img)

    red = img[:, :, 0]
    green = img[:, :, 1]
    blue = img[:, :, 2]

    # calculate the difference between green band with other two bands
    green_red_diff = green - red
    green_blue_diff = green - blue

    green_pixels = green_red_diff + green_blue_diff

    red_thre_img_u = red < 0.6
    green_thre_img_u = green < 0.9
    blue_thre_img_u = blue < 0.6

    shadow_red_u = red < 0.3
    shadow_green_u = green < 0.3
    shadow_blue_u = blue < 0.3

    green_img_thre = red_thre_img_u * blue_thre_img_u * green_thre_img_u
    green_img_shadow = shadow_red_u * shadow_green_u * shadow_blue_u

    threshold = threshold_otsu(green_pixels)

    if threshold > 0.1:
        threshold = 0.1
    elif threshold < 0.05:
        threshold = 0.05

    green_mask = green_pixels > threshold
    green_shadow_mask = green_pixels > 0.05
    green_img = green_img_thre * green_mask + green_shadow_mask * green_img_shadow

    # calculate the percentage of the green vegetation
    green_pixels_count = np.sum(green_img)
    green_percent = green_pixels_count / green_img.size * 100

    return green_percent


def green_view_computing(
    input_metadata: Path,
    output_greenview: Path,
    greenmonths: list,
    num_gsv_imgs: int,
    segment: bool,
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
    segment: bool, optional
        if the image will be segmented before calculating gvi
    api_key : str
        the Google Street View API key
    """
    fov = 360 // num_gsv_imgs
    pitch = 0

    headings = fov * np.array(range(3))

    gsv_images_path = output_greenview / 'images'
    gsv_images_path.mkdir(exist_ok=True, parents=True)

    # the input GSV info should be in a folder
    if not input_metadata:
        print('You should input a folder for GSV metadata')
        return
    else:
        metadata_files = input_metadata.glob('*.jsonl')

        for meta_file in tqdm(sorted(metadata_files), desc='processing gsv images'):
            with meta_file.open('r') as f:
                lines = f.readlines()

            # create empty lists, to store the information of panos,and remove duplicates
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

                # only use the months of green seasons
                if int(month) not in map(int, greenmonths):
                    continue
                else:
                    pano_ids.append(pano_id)
                    pano_dates.append(pano_date)
                    pano_longitudes.append(lng)
                    pano_latitudes.append(lat)

            # the output text file to store the green view and pano info
            gv_file_name = f'GV_{meta_file.stem}.jsonl'
            gv_file_path = output_greenview / gv_file_name

            # check whether the file already generated, if yes, skip.
            # Therefore, you can run several process at same time using this code.
            if gv_file_path.exists():
                continue

            # write the green view and pano info to txt
            with gv_file_path.open('w') as gv_indexes:
                for pano_id, pano_date, lat, lng in tqdm(
                    zip(pano_ids, pano_dates, pano_latitudes, pano_longitudes),
                    leave=False,
                    total=len(pano_ids)
                ):
                    # calculate the green view index
                    images = []
                    for heading in headings:
                        # classify the GSV images and calculate the GVI
                        image_data = requests.get(
                            'http://maps.googleapis.com/maps/api/streetview',
                            params={
                                'size': '400x400',
                                'pano': pano_id,
                                'fov': fov,
                                'heading': heading,
                                'pitch': pitch,
                                'sensor': 'false',
                                'key': api_key
                            },
                            stream=True
                        )
                        if image_data.ok:
                            img = Image.open(image_data.raw)
                            img_name = f'{pano_id}-{fov}-{heading}-{pitch}.png'
                            img.save(gsv_images_path / img_name)
                            img_array = np.array(img)
                            images.append(img_array)

                    # calculate the green view index by averaging percents from images
                    green_view_index = np.mean([
                        *map(lambda img: vegetation_classification(img, segment), images)
                    ])
                    # write the result and the pano info to the result txt file
                    json_line = {
                        'panoID': pano_id,
                        'panoDate': pano_date,
                        'longitude': lng,
                        'latitude': lat,
                        'greenview': green_view_index
                    }
                    gv_indexes.write(f'{json.dumps(json_line)}\n')


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
    parser.add_argument(
        '--segment',
        type=bool,
        default=True
    )

    args = parser.parse_args()

    API_KEY = os.getenv('MAPS_KEY')
    if API_KEY is None:
        raise Exception('MAPS_KEY not set')

    green_view_computing(
        args.input_metadata,
        args.output_greenview,
        args.greenmonth,
        args.num_images,
        args.segment,
        API_KEY,
    )
