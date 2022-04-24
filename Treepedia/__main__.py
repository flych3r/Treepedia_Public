if __name__ == '__main__':
    import argparse
    import asyncio
    import os
    from pathlib import Path

    from Treepedia.calculate_gvi import green_view_computing
    from Treepedia.clean_shapefile import create_points
    from Treepedia.greenview_shapefile import create_point_feature_ogr, read_gvi_data
    from Treepedia.images_collector import pano_images_collector
    from Treepedia.metadata_collector import pano_metadata_collector

    API_KEY = os.getenv('MAPS_KEY')
    if API_KEY is None:
        raise Exception('MAPS_KEY not set')

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('input_shapefile', type=Path)
    parser.add_argument('output_clean_shapefile', type=Path)
    parser.add_argument('output_metadata', type=Path)
    parser.add_argument('output_streetview', type=Path)
    parser.add_argument('output_greenview_index', type=Path)
    parser.add_argument('output_greenview_shapefile', type=Path)
    parser.add_argument('--min_dist', type=int, default=20)
    parser.add_argument('--num', type=int, default=500)
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

    create_points(
        args.input_shapefile,
        args.output_clean_shapefile,
        args.min_dist
    )
    asyncio.run(
        pano_metadata_collector(
            args.input_shapefile,
            args.output_metadata,
            args.num,
            API_KEY
        )
    )
    asyncio.run(
        pano_images_collector(
            args.output_metadata,
            args.output_streetview,
            args.greenmonth,
            args.num_images,
            API_KEY,
        )
    )
    green_view_computing(
        args.output_metadata,
        args.output_streetview,
        args.output_greenview_index,
        args.segment,
    )
    create_point_feature_ogr(
        args.output_greenview_shapefile,
        read_gvi_data(args.output_greenview_index),
        'greenView'
    )
