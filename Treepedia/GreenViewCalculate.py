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

import glob
import json
import os
import os.path
from pathlib import Path

import numpy as np
import requests
from PIL import Image
from skimage.filters import threshold_otsu
from tqdm.auto import tqdm


def VegetationClassification(image: np.array) -> float:
    '''
    This function is used to classify the green vegetation from GSV image,
    This is based on object based and otsu automatically thresholding method
    The season of GSV images were also considered in this function
        Img: the numpy array image
        return the percentage of the green vegetation pixels in the GSV image

    By Xiaojiang Li
    '''

    img = image / 255.0

    red = img[:, :, 0]
    green = img[:, :, 1]
    blue = img[:, :, 2]

    # calculate the difference between green band with other two bands
    green_red_Diff = green - red
    green_blue_Diff = green - blue

    ExG = green_red_Diff + green_blue_Diff
    diffImg = green_red_Diff * green_blue_Diff

    redThreImgU = red < 0.6
    greenThreImgU = green < 0.9
    blueThreImgU = blue < 0.6

    shadowRedU = red < 0.3
    shadowGreenU = green < 0.3
    shadowBlueU = blue < 0.3

    greenImg1 = redThreImgU * blueThreImgU * greenThreImgU
    greenImgShadow1 = shadowRedU * shadowGreenU * shadowBlueU

    threshold = threshold_otsu(ExG)

    greenImg2 = ExG > threshold
    greenImgShadow2 = ExG > 0.05
    greenImg = greenImg1 * greenImg2 + greenImgShadow2 * greenImgShadow1

    # calculate the percentage of the green vegetation
    greenPxlNum = np.sum(greenImg)
    greenPercent = greenPxlNum / greenImg.size * 100

    return greenPercent


# using 18 directions is too time consuming, therefore,
# here I only use 6 horizontal directions
# Each time the function will read a text, with 1000 records,
# and save the result as a single TXT
def GreenViewComputing_ogr_6Horizon(
    GSVinfoFolder: Path, outTXTRoot: Path, greenmonth: list, key: str
):
    """
    This function is used to download the GSV from the information provide
    by the gsv info txt, and save the result to a shapefile

    Required modules: StringIO, numpy, requests, and PIL

        GSVinfoTxt: the input folder name of GSV info txt
        outTXTRoot: the output folder to store result green result in txt files
        greenmonth: a list of the green season,
            for example in Boston, greenmonth = ['05','06','07','08','09']
        key: the API key
    last modified by Xiaojiang Li, MIT Senseable City Lab, March 25, 2018
    """

    # set a series of heading angle
    headingArr = 360 / 6 * np.array([0, 1, 2, 3, 4, 5])

    # number of GSV images for Green View calculation,
    # in my original Green View View paper, I used 18 images,
    # in this case, 6 images at different horizontal directions should be good.
    numGSVImg = len(headingArr) * 1.0
    pitch = 0

    # create a folder for GSV images and grenView Info
    if not os.path.exists(outTXTRoot):
        os.makedirs(outTXTRoot)
        os.makedirs(outTXTRoot / 'images')

    # the input GSV info should be in a folder
    if not os.path.isdir(GSVinfoFolder):
        print('You should input a folder for GSV metadata')
        return
    else:
        allTxtFiles = glob.glob(str(GSVinfoFolder / '*.jsonl'))
        for txtfilename in tqdm(allTxtFiles):
            lines = open(txtfilename, 'r')

            # create empty lists, to store the information of panos,and remove duplicates
            panoIDLst = []
            panoDateLst = []
            panoLonLst = []
            panoLatLst = []

            # loop all lines in the txt files
            for line in tqdm(lines, leave=False):
                metadata = json.loads(line)

                panoID = metadata['panoID']
                panoDate = metadata['panoDate']
                month = panoDate.split('-')[-1]
                lng = metadata['longitude']
                lat = metadata['latitude']

                # only use the months of green seasons
                if int(month) not in map(int, greenmonth):
                    continue
                else:
                    panoIDLst.append(panoID)
                    panoDateLst.append(panoDate)
                    panoLonLst.append(lng)
                    panoLatLst.append(lat)

            # the output text file to store the green view and pano info
            txtfile = txtfilename.split('/')[-1].split('.')[0]
            gvTxt = f'GV_{os.path.basename(txtfile)}.jsonl'
            GreenViewTxtFile = os.path.join(outTXTRoot, gvTxt)

            # check whether the file already generated, if yes, skip.
            # Therefore, you can run several process at same time using this code.
            if os.path.exists(GreenViewTxtFile):
                continue

            # write the green view and pano info to txt
            with open(GreenViewTxtFile, 'w') as gvResTxt:
                for panoID, panoDate, lat, lng in tqdm(
                    zip(panoIDLst, panoDateLst, panoLonLst, panoLatLst),
                    leave=False,
                    total=len(panoIDLst)
                ):
                    # calculate the green view index
                    greenPercent = 0.0

                    for heading in headingArr:
                        # let the code to pause by 1s, in order to not go
                        # over data limitation of Google quota
                        # time.sleep(1)

                        # classify the GSV images and calcuate the GVI
                        try:
                            image_data = requests.get(
                                'http://maps.googleapis.com/maps/api/streetview',
                                params={
                                    'size': '400x400',
                                    'pano': panoID,
                                    'fov': 60,
                                    'heading': heading,
                                    'pitch': pitch,
                                    'sensor': 'false',
                                    'key': key
                                },
                                stream=True
                            )
                            img = Image.open(image_data.raw)
                            if np.random.rand() < 0.2:
                                img.save(outTXTRoot / f'{panoID}-{heading}-{pitch}.png')
                            img_array = np.array(img)
                            percent = VegetationClassification(img_array)
                            greenPercent = greenPercent + percent

                        # if the GSV images are not download successfully or failed
                        # to run, then return a null value
                        except Exception:
                            greenPercent = -1000
                            break

                    # calculate the green view index by averaging percents from images
                    greenViewVal = greenPercent / numGSVImg
                    # write the result and the pano info to the result txt file
                    panoID
                    jsonLine = {
                        'panoID': panoID,
                        'panoDate': panoDate,
                        'longitude': lng,
                        'latitude': lat,
                        'greenview': greenViewVal
                    }
                    gvResTxt.write(f'{json.dumps(jsonLine)}\n')


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

    args = parser.parse_args()

    key = os.getenv('MAPS_KEY')
    if key is None:
        raise Exception('MAPS_KEY not set')

    GreenViewComputing_ogr_6Horizon(
        args.input_metadata,
        args.output_greenview,
        args.greenmonth,
        key
    )
