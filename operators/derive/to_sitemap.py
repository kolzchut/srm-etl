import datetime
import tempfile

import dataflows as DF
from dataflows_ckan import dump_to_ckan
from datapackage import Package

from conf import settings
from srm_tools.logger import logger

NUM_SITEMAPS = 2

def data_api_sitemap_flow():
    urls = DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/autocomplete/datapackage.json'),
        # DF.load(f'{settings.DATA_DUMP_DIR}/place_data/datapackage.json'),
        DF.filter_rows(lambda r: r['visible'] and not r['low'] and r['score'] > 1, resources='autocomplete'),
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.add_field('path', 'string', lambda r: '/s/{id}'.format(**r), resources='autocomplete'),
        # DF.add_field('path', 'string', lambda r: '/p/{key}'.format(**r), resources='places'),
        DF.add_field('path', 'string', lambda r: '/c/{card_id}'.format(**r), resources='card_data'),
        DF.set_type('path', transform=lambda v: v.replace("'", '&apos;').replace('"', '&quot;')),
        DF.printer()
    ).results(on_error=None)[0]
    today = datetime.date.today().isoformat()
    urls[0].insert(0, dict(path='/about/contact'))
    urls[0].insert(0, dict(path='/about/partners'))
    urls[0].insert(0, dict(path='/about/kolsherut'))
    urls[0].insert(0, dict(path='/'))
    _urls = []
    with tempfile.TemporaryDirectory() as tmpdir:
        idx = 0
        resources = []
        while len(urls) > 0 or len(_urls) > 0:
            if len(_urls) == 0:
                _urls = urls.pop(0)
            res_name = f'sitemap_{idx}' if idx > 0 else 'sitemap'
            base_filename = f'{res_name}.xml'
            filename = f'{tmpdir}/{base_filename}'
            with open(filename, 'w') as buff:
                buff.write('<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
                for row in _urls[:50000]:
                    buff.write('<url><loc>https://www.kolsherut.org.il{}</loc><lastmod>{}</lastmod></url>\n'.format(row['path'], today))
                buff.write('</urlset>')
                _urls = _urls[50000:]
            resources.append(dict(
                name=res_name,
                path=base_filename,
                format='xml',
                schema=dict(
                    fields=[dict(name='path', type='string')]
                )
            ))
            idx += 1
        assert len(resources) == NUM_SITEMAPS, f'Expected {NUM_SITEMAPS} resources, got {len(resources)}'
        dumper = dump_to_ckan(settings.CKAN_HOST, settings.CKAN_API_KEY, settings.CKAN_OWNER_ORG, force_format=False)
        datapackage = dict(
            name='sitemap',
            resources=resources,
        )
        dumper.datapackage = Package(datapackage)
        dumper.write_ckan_dataset(dumper.datapackage)
        print(dumper.datapackage.resources[0].descriptor)
        for resource in resources:
            path = resource['path']
            dumper.write_file_to_output(f'{tmpdir}/{path}', path)


def operator(*_):
    logger.info('Starting Sitemap Flow')

    # relational_sql_flow().process()
    data_api_sitemap_flow()

    logger.info('Finished Sitemap Flow')


if __name__ == '__main__':
    operator(None, None, None)
