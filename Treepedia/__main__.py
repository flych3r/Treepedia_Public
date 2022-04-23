if __name__ == '__main__':
    import argparse
    import os
    from pathlib import Path

    from Treepedia.calculate_gvi import green_view_computing
    from Treepedia.clean_shapefile import create_points
    from Treepedia.greenview_shapefile import create_point_feature_ogr, read_gvi_data
    from Treepedia.metadata_collector import pano_metadata_collector

    API_KEY = os.getenv('MAPS_KEY')
    if API_KEY is None:
        raise Exception('MAPS_KEY not set')

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_clean_shapefile', type=Path)
    parser.add_argument('output_metadata', type=Path)
    parser.add_argument('output_greenview', type=Path)
    parser.add_argument('output_greenview_shapefile', type=Path)
    parser.add_argument('--min_dist', type=int, default=20)
    parser.add_argument('--num', type=int, default=1000)
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

    create_points(
        args.input_shapefile,
        args.output_clean_shapefile,
        args.min_dist
    )
    pano_metadata_collector(
        args.output_clean_shapefile, args.output_metadata, API_KEY, args.num
    )
    green_view_computing(
        args.output_metadata,
        args.output_greenview,
        args.greenmonth,
        API_KEY,
        args.num_images
    )
    create_point_feature_ogr(
        args.output_greenview_shapefile,
        read_gvi_data(args.output_greenview),
        'greenView'
    )
