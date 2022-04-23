"""
This script is used to convert the green view index results saved in txt to Shapefile
considering the facts many people are more comfortable with shapefile and GIS
Copyright(C) Xiaojiang Li, Ian Seiferling, Marwa Abdulhai, Senseable City Lab, MIT
"""

import json
from pathlib import Path

from osgeo import ogr, osr
from tqdm.auto import tqdm


def read_gvi_data(input_gvi_data: Path) -> list:
    """
    This function is used to read the information in text files or folders
    the fundtion will remove the duplicate sites and only select those sites
    have GSV info in green month.

    Parameters
    ----------
    input_gvi_data : Path
        path to file with gvi

    Returns
    -------
    list
        list of dictionary with gvi data
    """
    unique_pano_ids = set()
    gvi_data = []

    for gvi_file in input_gvi_data.glob('*.jsonl'):
        with gvi_file.open('r') as f:
            lines = f.readlines()

        for line in lines:
            data = json.loads(line)

            pano_id = data['panoID']

            # remove the duplicated panorama id
            if pano_id not in unique_pano_ids:
                gvi_data.append(data)
                unique_pano_ids.add(pano_id)

    return gvi_data


def create_point_feature_ogr(
    output_shapefile: Path,
    gvi_data: list,
    layer_name: str
):
    """
    Create a shapefile based on the template of inputShapefile
    This function will delete existing outpuShapefile and create
    a new shapefile containing points with
    panoID, panoDate, and green view as respective fields.

    Parameters
    ----------
    output_shapefile: Path
        file to save shapefile with greenview points
    gvi_data: list
        green view index data
    layer_name: str
        name of layer

    Copyright(c) Xiaojiang Li, Senseable city lab
    """
    # create shapefile and add the above chosen random points to the shapfile
    driver = ogr.GetDriverByName('ESRI Shapefile')

    # create new shapefile
    if output_shapefile.exists():
        driver.DeleteDataSource(str(output_shapefile))

    data_source = driver.CreateDataSource(str(output_shapefile))
    target_spatial_ref = osr.SpatialReference()
    target_spatial_ref.ImportFromEPSG(4326)

    out_layer = data_source.CreateLayer(layer_name, target_spatial_ref, ogr.wkbPoint)

    # create a field
    id_field = ogr.FieldDefn('PntNum', ogr.OFTInteger)
    pano_id_field = ogr.FieldDefn('panoID', ogr.OFTString)
    pano_date_field = ogr.FieldDefn('panoDate', ogr.OFTString)
    green_view_field = ogr.FieldDefn('greenView', ogr.OFTReal)
    out_layer.CreateField(id_field)
    out_layer.CreateField(pano_id_field)
    out_layer.CreateField(pano_date_field)
    out_layer.CreateField(green_view_field)

    for idx, data in tqdm(enumerate(gvi_data), desc='creating greenview shapefile'):
        # create point geometry
        point = ogr.Geometry(ogr.wkbPoint)

        pano_id = data['panoID']
        pano_date = data['panoDate']
        lng = data['longitude']
        lat = data['latitude']
        gvi = data['greenview']

        point.AddPoint(float(lng), float(lat))

        # Create the feature and set values
        feature_defn = out_layer.GetLayerDefn()
        out_feature = ogr.Feature(feature_defn)
        out_feature.SetGeometry(point)
        out_feature.SetField('PntNum', idx)
        out_feature.SetField('panoID', pano_id)
        out_feature.SetField('panoDate', pano_date)
        out_feature.SetField('greenView', float(gvi))

        out_layer.CreateFeature(out_feature)
        out_feature.Destroy()

    data_source.Destroy()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_greenview', type=Path)
    parser.add_argument('output_shapefile', type=Path)

    args = parser.parse_args()

    layer_name = 'greenView'

    greenview_data = read_gvi_data(args.input_greenview)

    create_point_feature_ogr(
        args.output_shapefile,
        greenview_data,
        layer_name
    )

    print('Done!!!')
