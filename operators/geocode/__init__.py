import requests
import os
import json

import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from pyproj import Transformer
import geocoder

from conf import settings
from srm_tools.logger import logger


def geocode(session):
    transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
    def func(row):
        keyword = row.get('alternate_address', row.get('id'))
        if not keyword:
            return
        geocode_req = dict(
            keyword=keyword, type=0,
        )
        resp = session.post(settings.GOVMAP_GEOCODE_API, json=geocode_req)
        if resp.status_code != 200:
            logger.error(f'{geocode_req}')
            logger.error(f'{resp.status_code}: {resp.content}')
            assert False
        resp = resp.json()

        row['status'] = 'VALID'
        if resp['status'] == 0 and resp['errorCode'] == 0:
            assert 'data' in resp and len(resp['data']) > 0, str(resp)
            resp = resp['data'][0]
            assert resp['ResultType'] in (1, ), str(resp)
            assert resp['DescLayerID'] in ('POI_MID_POINT', 'ADDR_V1', 'NEIGHBORHOODS_AREA', 'SETL_MID_POINT', 'STREET_MID_POINT'), str(resp)
            row['accuracy'] = resp['DescLayerID']
            row['provider'] = 'govmap'
            row['resolved_address'] = resp['ResultLable']
            row['resolved_lon'], row['resolved_lat'] = transformer.transform(resp['X'], resp['Y'])
        else:
            resp = geocoder.google(keyword, language='he', key=settings.GOOGLE_MAPS_API_KEY)
            if resp.ok:
                row['accuracy'] = resp.accuracy
                row['provider'] = 'google'
                row['resolved_address'] = resp.address
                row['resolved_lon'], row['resolved_lat'] = resp.lng, resp.lat
            else:
                row['status'] = 'NOT_FOUND'
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


def operator(*_):
    print('GEOCODING')
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='locations'),
        DF.filter_rows(lambda r: any((not r.get(f)) for f in ('resolved_lat', 'resolved_lon'))),
        DF.filter_rows(lambda r: r['status'] not in ('NOT_FOUND', )),
        DF.select_fields([AIRTABLE_ID_FIELD, 'id', 'status', 'provider', 'alternate_address', 'accuracy', 'resolved_lat', 'resolved_lon', 'resolved_address']),
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


if __name__ == '__main__':
    operator()
