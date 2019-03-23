from ast import literal_eval
import itertools
import math
import multiprocessing
import os
import requests
from urllib.parse import urlencode

from geopy.distance import geodesic
import numpy as np
import polyline
import redis

BASE_URL = "https://maps.googleapis.com/maps/api/distancematrix/json?"
MODE_OPTIONS = ("driving", "walking", "bicycling", "transit")
TRAFFIC_MODEL_OPTIONS = ("best_guess", "pessimistic", "optimistic")
UNITS_OPTIONS = ("metric", "imperial")

CURITIBA_CORNERS = ((25.5561112, -49.376088), (-25.3083294, -49.1437658))
BRASILIA_CORNERS = ((-16.050786, -48.277373), (-15.500253, -47.367992))
SALVADOR_CORNERS = ((-13.017645, -38.536107), (-12.792186, -38.306034))
TOKYO_CORNERS = ((35.182396, 138.857943), (36.944868, 141.058548))
KALGARY_CORNERS = ((51.183678, -114.280325), (50.867719, -113.853606))
WINNIPEG_CORNERS = ((49.770565, -97.338883), (49.971286, -96.937157))
LAS_VEGAS_CORNERS = ((35.921432, -115.357440), (36.309937, -114.919675))
KANSAS_CITY_CORNERS = ((38.721462, -95.015595), (39.395410, -94.192277))
DUBLIN_CORNERS = ((53.243307, -6.466802), (53.457907, -6.079077))
SAO_PAULO_CORNERS = ((-23.808418, -46.962472), (-23.357473, -46.231374))

try:
    API_KEY = os.environ["MAPS_API_KEY"]
except KeyError:
    print("Warning! No Google Maps API key detected")


try:
    REDIS_HOST = os.environ["REDIS_HOST"]
except KeyError:
    REDIS_HOST = 'localhost'


db = redis.Redis(host=REDIS_HOST, port=6379, db=0)


def build_distance_matrix_url(
        origins: tuple, destinations: tuple, units="metric", arrival_time=None,
        departure_time=None, traffic_model="best_guess",
        mode="driving", precision=5, escape=False, api_key=None) -> str:

    query = {
        'origins': f"enc:{polyline.encode(origins, precision)}:",
        'destinations': f"enc:{polyline.encode(destinations, precision)}:",
        'key': api_key if api_key is not None else API_KEY,
    }

    if units is not None and units in UNITS_OPTIONS:
        query["units"] = units

    if arrival_time is not None:
        query["arrival_time"] = arrival_time

    if departure_time is not None:
        query["departure_time"] = departure_time

    if traffic_model is not None and traffic_model in TRAFFIC_MODEL_OPTIONS \
            and traffic_model != 'best_guess':
        query["traffic_model"] = traffic_model

    if mode is not None and mode in MODE_OPTIONS:
        query["mode"] = mode

    if escape:
        url = BASE_URL + urlencode(query)
    else:
        query = "&".join(f"{key}={value}" for key, value in query.items())
        url = BASE_URL + query

    if len(url) > 8192:
        raise ValueError(f'Url {url} is too long (len > 8192)')

    return url


def sample_coord_square(
        first_corner: tuple, second_corner: tuple, samples_per_lat=100,
        samples_per_lon=100) -> list:

    lat_samples = np.linspace(
        min(first_corner[0], second_corner[0]),
        max(first_corner[0], second_corner[0]),
        samples_per_lat)

    lon_samples = np.linspace(
        min(first_corner[1], second_corner[1]),
        max(first_corner[1], second_corner[1]),
        samples_per_lon)

    return list(itertools.product(lat_samples, lon_samples))


def distance(coords: tuple) -> float:
    return geodesic(coords[0], coords[1]).km * 1000


def get_coord_key_str(origin: tuple, dest: tuple) -> str:
    return f"(({origin[0]:.5f}, {origin[1]:.5f}), " \
           f"({dest[0]:.5f}, {dest[1]:.5f}))"


def get_coord_pair_dict(origin: tuple, dest: tuple) -> dict:
    global db
    key = get_coord_key_str(origin, dest)
    value = db.hgetall(key)

    return value if value is None else {}


def update_coord_pair_content(
        origin: tuple, dest: tuple, new_dict: dict) -> dict:
    global db
    key = get_coord_key_str(origin, dest)

    if db.hmset(key, new_dict):
        return new_dict


def update_coord_pair_value(origin: tuple, dest: tuple, key, value) -> dict:
    global db
    current_dict = get_coord_pair_dict(origin, dest)
    current_dict[key] = value
    return update_coord_pair_content(origin, dest, current_dict)


def generate_dicts(args: tuple) -> list:
    return [
        update_coord_pair_content(
            args[0][0], args[0][1],
            {
                'geo_dist': args[1],
                'origin_coord': str(args[0][0]),
                'destination': str(args[0][1]),
            }),

        update_coord_pair_content(
            args[0][1], args[0][0],
            {
                'geo_dist': args[1],
                'origin_coord': str(args[0][0]),
                'destination': str(args[0][1]),
            }),
    ]


def build_distances_dict(
        coord_1: tuple, coord_2: tuple, samples_per_lat=32,
        samples_per_lon=32) -> tuple:
    number_of_points = samples_per_lat * samples_per_lon

    expected_size = \
        math.factorial(number_of_points) / math.factorial(number_of_points - 2)

    print(f'Number of points: {int(number_of_points)}')
    print(f'Expected output size: {int(expected_size)}')

    coords = sample_coord_square(
        coord_1, coord_2, samples_per_lat=samples_per_lat,
        samples_per_lon=samples_per_lon)

    orig_dest = tuple(itertools.permutations(coords, 2))

    pool = multiprocessing.Pool()
    dists = pool.map(distance, orig_dest)

    args = [(orig_dest[i], dists[i]) for i in range(len(orig_dest))]

    return pool.map(generate_dicts, args)

    # for i in range(len(orig_dest)):
    #     update_coord_pair_content(
    #         orig_dest[i][0], orig_dest[i][1],
    #         {
    #             'geo_dist': dists[i],
    #             'origin_coord': str(orig_dest[i][0]),
    #             'destination': str(orig_dest[i][1]),
    #         })

    #     update_coord_pair_content(
    #         orig_dest[i][1], orig_dest[i][0],
    #         {
    #             'geo_dist': dists[i],
    #             'origin_coord': str(orig_dest[i][0]),
    #             'destination': str(orig_dest[i][1]),
    #         })


def clear_db() -> None:
    global db

    for key in db.scan_iter():
       db.delete(key)


# geo_data.build_distances_dict(((-23.808418, -46.962472), (-23.357473, -46.231374))
# geo_data.get_coord_pair_dict((-23.357473, -46.231374), (-23.357473, -46.26183641666667))
