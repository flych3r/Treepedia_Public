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
from multiprocessing import Pool
from pathlib import Path

import numpy as np
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


def process_images(metadata: dict, segment: bool, input_streetview: Path) -> dict:
    """
    Calculates the GVI for a pano

    Parameters
    ----------
    metadata : dict
        pano data
    segment : bool
        if image will be segmented
    input_streetview : Path
        path to the pano images

    Returns
    -------
    dict
        data with greenview index
    """
    pano_id = metadata['panoID']

    pano_images_path: Path = (input_streetview / pano_id)
    images_paths = pano_images_path.glob('*.png')
    images = [
        np.array(Image.open(p)) for p in images_paths
    ]

    # calculate the green view index by averaging percents from images
    green_view_index = np.mean([
        *map(lambda img: vegetation_classification(img, segment), images)
    ])
    metadata['greenview'] = green_view_index
    return metadata


def green_view_computing(
    input_metadata: Path,
    input_streetview: Path,
    output_greenview_index: Path,
    segment: bool,
):
    """
    This function is used to download the GSV from the information provide
    by the gsv info txt, and save the result to a shapefile

    Parameters
    ----------
    input_metadata: Path
        the input folder name of GSV info
    input_streetview : Path
        the input folder with streetview images
    output_greenview_index : Path
        the output folder to store result green result
    segment: bool, optional
        if the image will be segmented before calculating gvi
    """
    metadata_files = input_metadata.glob('*.jsonl')
    output_greenview_index.mkdir(exist_ok=True)

    for meta_file in tqdm(sorted(metadata_files), desc='processing gsv images'):
        with meta_file.open('r') as f:
            lines = f.readlines()

        panos_metadata = list(map(json.loads, lines))
        gv_file_name = f'GV_{meta_file.stem}.jsonl'
        gv_file_path = output_greenview_index / gv_file_name

        # check whether the file already generated, if yes, skip.
        # Therefore, you can run several process at same time using this code.
        if gv_file_path.exists():
            continue

        threads = os.cpu_count()

        with Pool(threads) as p:
            jobs = [
                p.apply_async(
                    process_images, args=(metadata, segment, input_streetview)
                )
                for metadata in panos_metadata
            ]
            metadata_gvi = [job.get() for job in tqdm(jobs, leave=False)]

        # # write the result and the pano info to the result txt file
        with gv_file_path.open('w') as gv_indexes:
            # write the green view and pano info to txt
            [gv_indexes.write(f'{json.dumps(gvi)}\n') for gvi in metadata_gvi]


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_metadata', type=Path)
    parser.add_argument('input_streetview', type=Path)
    parser.add_argument('output_greenview_index', type=Path)
    parser.add_argument(
        '--segment',
        type=bool,
        default=True
    )

    args = parser.parse_args()

    green_view_computing(
        args.input_metadata,
        args.input_streetview,
        args.output_greenview_index,
        args.segment,
    )
