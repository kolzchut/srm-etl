import re
import os
import requests
import subprocess
import shutil
import tempfile
import dataflows as DF
import fiona
import pghstore
from shapely.geometry import shape
from shapely.ops import unary_union
from thefuzz import process as fuzz_process, fuzz


SOURCE_OSM_URL = 'https://download.geofabrik.de/asia/israel-and-palestine-latest.osm.pbf'
GPKG_FILENAME = 'places.gpkg'

def run_ogr2ogr(from_filename, to_filename, tmpdirname):
    to_filename = os.path.join(tmpdirname, to_filename)
    subprocess.run(['ogr2ogr', to_filename, from_filename])
    return to_filename


def download_url_to_tmpfile(from_url, tmpdirname):
    r = requests.get(from_url, stream=True)
    r.raise_for_status()
    tmpfilename = os.path.join(tmpdirname, 'osm.pbf')
    with open(tmpfilename, 'wb') as f:
        shutil.copyfileobj(r.raw, f)
    return tmpfilename
    
RESOURCE_ID = '5c78e9fa-c2e2-4771-93ff-7f400a12f7ba'

def download_datagov(resource_id):
    yield from requests.get(f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}').json()['result']['records']


def include_anyways(names):
    OKAY_PREFIXES = [
        'מועצה אזורית', 'א-', 'אבו ', 'אום ', 'אל ', 'אל-', 'גבעת ', 
    ]
    for name in names:
        for prefix in OKAY_PREFIXES:
            if name.startswith(prefix):
                return True
    return False


HEB = re.compile(r'[א-ת]')
ENG = re.compile(r'[a-zA-Z]')
HEB_STRS = re.compile(r'[א-ת]+')


def is_heb(s):
    return len(HEB.findall(s)) > len(s)/2 and not len(ENG.findall(s))


def names(r):
    ret = set(v for v in r['properties'].values() if isinstance(v, str) and is_heb(v))
    ret = sorted(set(filter(None, map(fix_osm_name, ret))))
    return ret


def key(vv):
    return '_'.join(HEB_STRS.findall(' '.join(vv)))


def bounds(r):
    geometry = r['geometry']
    geometry = unary_union([shape(g) for g in geometry])
    bounds = geometry.bounds
    return bounds


def match_officials(official_places, official_place_map=None):
    def func(r):
        ret = set()
        for name in r['name']:
            if not name: continue
            bests = fuzz_process.extractBests(name, official_places, score_cutoff=86, scorer=fuzz.UWRatio)
            ret.update(bests)
        if official_place_map is not None:
            for name, score in ret:
                official_place_map.setdefault(name, set()).add((score, r['key']))
        return sorted(ret, reverse=True, key=lambda i: i[1])
    return func


def select_officials(mapping):
    def func(row):
        key = row['key']
        officials = row['official']
        officials = [o[0] for o in officials if o[1] >= 87 and mapping.get(o[0]) == key]
        row['official'] = officials[0] if officials else None
    return func


def fix_official_names(row):
    name = row['name']
    name = name.replace('(שבט)', '')
    name = name.replace('(יישוב)', '')
    name = name.replace('(ישוב)', '')
    name = name.replace('(מושב)', '')
    name = name.replace('(כפר נוער)', '')
    name = name.replace('(קבוצה)', '')
    name = name.replace(' - ', '-')
    name = name.replace(' -', '-')
    name = name.replace('- ', '-')
    name = name.strip()
    row['name'] = name


def fix_osm_name(name):
    name = name.replace(' - ', '-')
    name = name.replace(' -', '-')
    name = name.replace('- ', '-')
    name = name.replace('"', '״')
    name = name.replace("'", '׳')
    name = name.replace('(מושב)', '')
    name = name.replace('(קיבוץ)', '')
    name = name.replace('(הרוס)', '')
    if name.startswith('קרית '):
        name = 'קריית ' + name[5:]
    if name.startswith('נוה '):
        name = 'נווה ' + name[4:]
    if name.endswith('ייה'):
        name = name[:-3] + 'יה'
    if name == 'שהם': return 'שוהם'
    BAD_WORDS = [
        'שכונת', 'דיסטריקט', 'שדרות ', 'כפר הנוער ', 'יורדי ים', 'בית חולים', 'נפת שכם', 'יישוב בדואי'
    ]
    for word in BAD_WORDS:
        if word in name: return None
    if ';' in name: return None
    if any(x in name for x in '0123456789'): return None
    if len(name) > 20 or len(name.split(' ')) > 7: return None
    name = name.strip()
    return name


def main():
    # with tempfile.TemporaryDirectory(delete=False) as tmpdirname:
    #     print(tmpdirname)
    #     osm_file = download_url_to_tmpfile(SOURCE_OSM_URL, tmpdirname)
    #     run_ogr2ogr(osm_file, GPKG_FILENAME, tmpdirname)

    if tmpdirname := '/var/folders/r8/5zwhvh9j3jzd1g9rxwhg3kgw0000gn/T/tmp79oxkp88':
        source = map(lambda i: i.__geo_interface__, fiona.open(f'israel-and-palestine.osm.gpkg', layer='multipolygons').filter())
        RANKS = {'city', 'town', 'village', 'hamlet'}

        dp, _ = DF.Flow(
            source,
            DF.update_resource(-1, name='place_bounds_he'),
            DF.add_field('tags', 'string', lambda r: r['properties'].get('other_tags') or ''),
            lambda row: row['properties'].update(pghstore.loads(row['tags'])),
            DF.add_field('place', 'string', lambda r: r['properties'].get('place')),
            DF.add_field('landuse', 'string', lambda r: r['properties'].get('landuse')),
            DF.add_field('boundary', 'string', lambda r: r['properties'].get('boundary')),
            DF.add_field('population', 'string', lambda r: r['properties'].get('population')),
            DF.add_field('name', 'array', names),
            DF.filter_rows(lambda r: r['place'] in RANKS or r['landuse'] == "residential" or bool(r['population'])),
            DF.filter_rows(lambda r: r['geometry'] and 'Polygon' in r['geometry']['type']),
            DF.filter_rows(lambda r: bool(r['name'])),
            DF.add_field('key', 'string', lambda r: key(r['name'])),
            DF.select_fields(['place', 'landuse', 'name', 'key', 'geometry', 'population']),
            DF.join_with_self('place_bounds_he', ['key'], dict(
                place=None,
                landuse=None,
                population=None,
                name=None,
                key=None,
                geometry=dict(aggregate='array')
            )),
            DF.printer(num_rows=10),
            DF.checkpoint('generate_places', checkpoint_path=tmpdirname + '/checkpoint'),
            DF.filter_rows(lambda r: 'אילת' in r['key']),
            DF.printer(num_rows=10),
        ).process()

        official_places = DF.Flow(
            download_datagov(RESOURCE_ID),
            DF.filter_rows(lambda r: bool(r['שם_ישוב']) and r['סמל_ישוב'] != '0'),
            DF.select_fields (['שם_ישוב']),
            DF.rename_fields({'שם_ישוב': 'name'}),
            DF.printer(),
            fix_official_names,
            DF.checkpoint('official_places', checkpoint_path=tmpdirname + '/checkpoint'),
        ).results()[0][0]
        official_places = [r['name'] for r in official_places]
        official_place_map = {r: set() for r in official_places}

        DF.Flow(
            DF.checkpoint('generate_places', checkpoint_path=tmpdirname + '/checkpoint'),
            DF.add_field('official', 'array', match_officials(official_places, official_place_map)),
            DF.checkpoint('with_officials', checkpoint_path=tmpdirname + '/checkpoint'),
            DF.filter_rows(lambda r: 'אילת' in r['key']),
            DF.printer(num_rows=10),
        ).process()

        mapping = {}
        for k, v in official_place_map.items():
            v = sorted(v, reverse=True)
            if len(v) > 0:
                top = v[0]
                if top[0] >= 87:
                    mapping[k] = top[1]


        DF.Flow(
            DF.checkpoint('with_officials', checkpoint_path=tmpdirname + '/checkpoint'),
            select_officials(mapping),
            DF.filter_rows(lambda r:  bool(r['official'] or r['place'] in RANKS or include_anyways(r['name']))),
            DF.add_field('bounds', 'array', bounds),
            DF.select_fields(['key', 'place', 'name', 'population', 'bounds']),
            DF.update_resource(-1, path='place_bounds_he.csv'),
            DF.set_type('bounds', **{'es:index': False, 'es:itemType': 'number'}),
            DF.set_type('place', **{'es:keyword': True}),
            DF.set_type('name', **{'es:itemType': 'string'}),
            DF.set_type('key', **{'es:keyword': True}),
            DF.set_primary_key(['key']),
            DF.dump_to_zip('data/place_bounds_he.zip'),
            DF.printer(num_rows=100)
        ).process()
        dp.resources[0].descriptor

if __name__ == '__main__':
    main()