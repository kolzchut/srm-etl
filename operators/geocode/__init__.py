import requests
import os
import json

import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable

from pyproj import Transformer
import geocoder

from srm_tools.logger import logger

from conf import settings


def geocode(session):
    transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
    def func(row):
        key = row.get('key')
        geocode_req = dict(
            keyword=key, type=0,
        )
        resp = session.post(
            settings.GOVMAP_GEOCODE_API_ENTRYPOINT, json=geocode_req
        ).json()
        # print(key, row, any((not row.get(f)) for f in ('resolved_lat', 'resolved_lon')), resp)
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
            resp = geocoder.google(key, language='he')
            if resp.ok:
                row['accuracy'] = resp.accuracy
                row['provider'] = 'google'
                row['resolved_address'] = resp.address
                row['resolved_lon'], row['resolved_lat'] = resp.lng, resp.lat
            else:
                row['status'] = 'NOT_FOUND'

    return func

def get_session():
    token = settings.GOVMAP_API_KEY
    auth_data = dict(
        api_token=token, user_token="", domain=settings.API_REQUEST_ORIGIN, token=""
    )
    headers = dict(
        auth_data=json.dumps(auth_data),
        Origin=settings.API_REQUEST_ORIGIN,
        Referer=settings.API_REQUEST_ORIGIN,
    )

    resp = requests.post(
        settings.GOVMAP_GEOCODE_AUTH_ENTRYPOINT,
        json=dict(),
        headers=headers,
    )
    # print(resp.status_code)
    # print(resp.content)
    headers = dict(
        auth_data=json.dumps(resp.json()),
        Origin=settings.API_REQUEST_ORIGIN,
        Referer=settings.API_REQUEST_ORIGIN,
    )

    session = requests.Session()
    session.headers.update(headers)
    return session


def operator(*_):
    DF.Flow(
        load_from_airtable(
            settings.AIRTABLE_BASE,
            settings.AIRTABLE_LOCATION_TABLE,
            settings.AIRTABLE_VIEW,
        ),
        DF.update_resource(-1, **{"name": "locations"}),
        DF.filter_rows(
            lambda r: any((not r.get(f)) for f in ("resolved_lat", "resolved_lon"))
        ),
        DF.filter_rows(lambda r: r["status"] not in ("NOT_FOUND",)),
        DF.set_type(
            "resolved_l.+",
            type="number",
            transform=lambda v: float(v) if v is not None else None,
        ),
        geocode(get_session()),
        DF.dump_to_path("geocode"),
        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE): {
                    "resource-name": "locations",
                    "typecast": True,
                }
            }
        ),
    ).process()


if __name__ == "__main__":
    operator()
