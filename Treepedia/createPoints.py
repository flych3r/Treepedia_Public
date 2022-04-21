"""
This program is used in the first step of the Treepedia project to get points along street
network to feed into GSV python scripts for metadata generation.

Copyright(C) Ian Seiferling, Xiaojiang Li, Marwa Abdulhai, Senseable City Lab, MIT
First version July 21 2017
"""

import argparse
import os
import os.path
from functools import partial
from pathlib import Path

import fiona
import pyproj
from fiona.crs import from_epsg
from shapely.geometry import mapping, shape
from shapely.ops import transform


def createPoints(input_shapefile: Path, output_shapefile: Path, min_dist: int):
    """
    This function will parse through the street network of provided city and
    clean all highways and create points every min_dist meters (or as specified) along
    the linestrings

    Parameters
    ----------
    input_shapefile: Path
        the input linear shapefile, must be in WGS84 projection, ESPG: 4326
    output_shapefile: Path
        the result point feature class
    min_dist: int
        the minimum distance between two created point

    last modified by Xiaojiang Li, MIT Senseable City Lab
    """

    s = {
        'trunk_link',
        'tertiary',
        'motorway',
        'motorway_link',
        'steps',
        None,
        ' ',
        'pedestrian',
        'primary',
        'primary_link',
        'footway',
        'tertiary_link',
        'trunk',
        'secondary',
        'secondary_link',
        'tertiary_link',
        'bridleway',
        'service',
    }

    # the temporaray file of the cleaned data
    root = os.path.dirname(input_shapefile)
    basename = 'clean_' + os.path.basename(input_shapefile)
    temp_cleanedStreetmap = os.path.join(root, basename)

    # if the tempfile exist then delete it
    if os.path.exists(temp_cleanedStreetmap):
        fiona.remove(temp_cleanedStreetmap, 'ESRI Shapefile')

    # clean the original street maps by removing highways, if it the street map
    # not from Open street data, users'd better to clean the data themselves
    with (
        fiona.open(input_shapefile) as source,
        fiona.open(
            temp_cleanedStreetmap,
            'w',
            driver=source.driver,
            crs=source.crs,
            schema=source.schema,
        ) as dest,
    ):

        for feat in source:
            try:
                i = feat['properties']['highway']  # for the OSM street data
                if i in s:
                    continue
            except Exception:
                # if the street map is not osm, do nothing.
                # You'd better to clean the street map,
                # if you don't want to map the GVI for highways
                # get the field of the input shapefile and duplicate the input feature
                key = list(dest.schema['properties'].keys())[0]
                i = feat['properties'][key]
                if i in s:
                    continue
            dest.write(feat)

    schema = {
        'geometry': 'Point',
        'properties': {'id': 'int'},
    }

    # Create pointS along the streets
    with fiona.drivers():
        with fiona.open(
            output_shapefile,
            'w',
            crs=from_epsg(4326),
            driver='ESRI Shapefile',
            schema=schema,
        ) as output:
            for line in fiona.open(temp_cleanedStreetmap):
                first = shape(line['geometry'])
                first.length

                try:
                    # convert degree to meter, in order to split by distance in meter
                    # 3857 is pseudo WGS84 the unit is meter
                    project = partial(
                        pyproj.transform,
                        pyproj.Proj(init='EPSG:4326'),
                        pyproj.Proj(init='EPSG:3857'),
                    )

                    line2 = transform(project, first)
                    list(line2.coords)
                    dist = min_dist
                    for distance in range(0, int(line2.length), dist):
                        point = line2.interpolate(distance)

                        # convert the local projection back the the WGS84
                        # and write to the output shp
                        project2 = partial(
                            pyproj.transform,
                            pyproj.Proj(init='EPSG:3857'),
                            pyproj.Proj(init='EPSG:4326'),
                        )
                        point = transform(project2, point)
                        output.write(
                            {'geometry': mapping(point), 'properties': {'id': 1}}
                        )
                except Exception:
                    print('You should make sure the input shapefile is WGS84')
                    return

    print('Process Complete')

    # delete the temporary cleaned shapefile
    fiona.remove(temp_cleanedStreetmap, 'ESRI Shapefile')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_shapefile', type=Path)
    parser.add_argument('--min_dist', type=int, default=20)

    args = parser.parse_args()

    createPoints(args.input_shapefile, args.output_shapefile, args.min_dist)
