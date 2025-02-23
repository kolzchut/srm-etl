import requests
import os
import json

import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from pyproj import Transformer
import geocoder

from conf import settings
from operators.derive.helpers import ACCURATE_TYPES
from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on


def geocode(session):
    transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
    def func(row):
        keyword = row.get('alternate_address') or row.get('id')
        pluscode = len(keyword) > 4 and keyword[4] == '+'
        if not keyword:
            return
        row['status'] = 'VALID'
        row['resolved_lat'] = row['resolved_lon'] = None

        if keyword == 'שירות ארצי' or keyword == 'כל הארץ':
            row['accuracy'] = 'NATIONAL_SERVICE'
            row['provider'] = 'national'
            row['resolved_address'] = 'שירות ארצי'
            return

        if pluscode:
            resp = dict(status='plus')
        else:
            geocode_req = dict(
                keyword=keyword, type=0,
            )
            resp = session.post(settings.GOVMAP_GEOCODE_API, json=geocode_req)
            if resp.status_code not in (200, 404):
                logger.error(f'{geocode_req}')
                logger.error(f'{resp.status_code}: {resp.content}')
                assert False
            try:
                resp = resp.json()
            except json.decoder.JSONDecodeError:
                resp = dict(status=None, errorCode=None)

        if resp['status'] == 0 and resp['errorCode'] == 0:
            assert 'data' in resp and len(resp['data']) > 0, str(resp)
            resp = resp['data'][0]
            assert resp['ResultType'] in (1, ), str(resp)
            accuracy = resp['DescLayerID'].replace('NEW', '').strip('_')
            assert accuracy in (
                'POI_MID_POINT', 'ADDR_V1', 'NEIGHBORHOODS_AREA', 'SETL_MID_POINT', 'STREET_MID_POINT', 'ADDRESS_POINT', 'ADDRESS'), str(resp)
            row['accuracy'] = accuracy
            row['provider'] = 'govmap'
            row['resolved_address'] = resp['ResultLable']
            row['resolved_lon'], row['resolved_lat'] = transformer.transform(resp['X'], resp['Y'])

        if any(row.get(f) is None for f in ('resolved_lat', 'resolved_lon', 'resolved_address')) \
            or row.get('accuracy') not in ACCURATE_TYPES:
            print('ROW', row)
            resp = geocoder.google(keyword, language='he', key=settings.GOOGLE_MAPS_API_KEY)
            if resp.ok:
                accuracy = resp.accuracy
                address = resp.address
                if accuracy == 'GEOMETRIC_CENTER':
                    if resp.quality == 'establishment':
                        accuracy = 'POI_MID_POINT'
                    elif resp.quality == 'plus_code':
                        accuracy = 'ROOFTOP'
                    else:
                        print(accuracy, resp.quality)
                if pluscode:
                    accuracy = 'ADDR_V1'
                    address = row.get('id')
                row['accuracy'] = accuracy
                row['provider'] = 'google'
                row['resolved_address'] = address
                row['resolved_city'] = (
                    resp.raw.get('locality', {}).get('long_name') or 
                    resp.raw.get('administrative_area_level_2', {}).get('long_name') or 
                    resp.city
                )
                row['resolved_lon'], row['resolved_lat'] = resp.lng, resp.lat
            else:
                row['status'] = 'NOT_FOUND'

        if row.get('resolved_lat') and row.get('resolved_lon') and not row.get('resolved_city'):
            resp = geocoder.google('{}, {}'.format(row['resolved_lat'], row['resolved_lon']), language='he', key=settings.GOOGLE_MAPS_API_KEY)
            if resp.ok:
                row['resolved_city'] = (
                    resp.raw.get('locality', {}).get('long_name') or 
                    resp.raw.get('administrative_area_level_2', {}).get('long_name') or 
                    resp.city
                )
            if not row.get('resolved_city'):
                row['resolved_city'] = 'unknown'

        if row.get('resolved_address'):
            if row['resolved_address'].endswith(', ישראל'):
                row['resolved_address'] = row['resolved_address'][:-7]
            row['resolved_address'] = row['resolved_address'].replace(' | ', ', ')
    return func

def get_session():
    try:
        token = settings.GOVMAP_API_KEY
        auth_data = dict(
            api_token=token, user_token='', domain=settings.GOVMAP_REQUEST_ORIGIN, token=''
        )
        headers = dict(
            auth_data=json.dumps(auth_data),
            Origin=settings.GOVMAP_REQUEST_ORIGIN,
            Referer=settings.GOVMAP_REQUEST_ORIGIN,
        )

        resp = requests.post(settings.GOVMAP_AUTH,
                        json=dict(),
                        headers=headers)
        # print(resp.status_code)
        # print(resp.content)
        headers = dict(
            auth_data=json.dumps(resp.json()),
            Origin=settings.GOVMAP_REQUEST_ORIGIN,
            Referer=settings.GOVMAP_REQUEST_ORIGIN,
        )
    except Exception as e:
        logger.error(e)
        headers = {}

    session = requests.Session()
    session.headers.update(headers)
    return session


def runGeocoding(*_):
    print('GEOCODING')
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='locations'),
        DF.filter_rows(lambda r: any((not r.get(f)) for f in ('resolved_lat', 'resolved_lon', 'resolved_city'))),
        DF.filter_rows(lambda r: r.get('status') not in ('NOT_FOUND', )),
        DF.select_fields([AIRTABLE_ID_FIELD, 'id', 'status', 'provider', 'alternate_address', 'accuracy', 'resolved_lat', 'resolved_lon', 'resolved_address', 'resolved_city']),
        DF.set_type('resolved_l.+', type='number', transform=lambda v: float(v) if v is not None else None),
        geocode(get_session()),
        dump_to_airtable({
            (settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE): {
                'resource-name': 'locations',
                'typecast': True
            }
        }, settings.AIRTABLE_API_KEY),
        DF.dump_to_path('geocode'),
    ).process()

def operator(*_):
    invoke_on(runGeocoding, 'Geocode')

if __name__ == '__main__':
    operator()
