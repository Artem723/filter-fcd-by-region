from shapely import wkt
from shapely import Point
import geopandas as gpd
import pandas as pd
import csv
from tqdm import tqdm
from shapely import MultiPolygon
import shapely
from pathlib import Path
import os
from os.path import isfile, join
import argparse
import zipfile

N_ROWS = 1000000



parser = argparse.ArgumentParser(
                    prog='FCD filter',
                    description='Filter all the FCD records that fall inside the polygons',
                    epilog='The result will be flattened to the output directory')

parser.add_argument('-i', '--input', dest='input_folder', help='input directory that will be processed recursively')
parser.add_argument('-o', '--output', dest='output_folder', help='output directory that will store the results of the filtering')
parser.add_argument('-f', '--filter_by', dest='buffer_path', help="A shapefile with the polygon geometries")

args = parser.parse_args()

buffers = gpd.read_file(args.buffer_path)

def process(chunk, input_f_name, chunk_c, n_rows, output_path):
    points = gpd.GeoDataFrame(chunk, geometry=gpd.points_from_xy(chunk.LAT, chunk.LON), crs='EPSG:4326')
    points_within_poly_idx = points.sindex.query(buffers.geometry, predicate='contains')
    indexes = list(map(lambda ind: ind + chunk_c*n_rows, points_within_poly_idx[1]))
    resulted_points = chunk.loc[indexes, :]
    resulted_points['radar'] = points_within_poly_idx[0]

    def mapping_ind_to_name(ind):
        return buffers.loc[ind, 'camera_id']

    resulted_points['radar'] = resulted_points['radar'].map(mapping_ind_to_name)
    
    resulted_points.to_csv(os.path.join(output_path, 'filtered_' + input_f_name + '.csv'), mode='a')


input_path = args.input_folder
output_path = args.output_folder
# files = [f for f in os.listdir(input_path) if isfile(join(input_path, f)) and f.endswith('.txt')]
files = list()
for d in os.walk(input_path):
    d_files = [(d[0], f) for f in d[2]]
    print(d_files)
    files.extend(d_files)

names = ['ID','LON','LAT','DIRECTION','SPEED','TIMESTAMP','STATE','GPS_QUALITY','TRIP_ID','PROTOCOL','CLASS','ODOMETER']
with open(os.path.join(output_path, 'skipped_files'), 'a') as skipped_f:
    for input_f_tuple in files:
        print('PROCESSING ', input_f_tuple[1])
        try:
            with pd.read_csv(join(input_f_tuple[0], input_f_tuple[1]), chunksize=N_ROWS, names=names) as reader:
                for count, chunk in enumerate(reader):
                    print(' - CHUNK ', count)
                    process(chunk, input_f_tuple[1], count, N_ROWS, output_path)
        except zipfile.BadZipFile:
            print("The file: {} is broken..", input_f_tuple[1])
            skipped_f.write("Broken archive " + input_f_tuple[1] + '\n')
        except pd.errors.ParserError:
            print("The file: {} is malformed..", input_f_tuple[1])
            skipped_f.write("Malformed records " + input_f_tuple[1] + '\n')
        except Exception as e:
            print("General exception: ", input_f_tuple[1])
            skipped_f.write("General exception " + input_f_tuple[1] + '\t')
            skipped_f.write(str(type(e)) + str(e) + '\n')

