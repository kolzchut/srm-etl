import dataflows as DF

url='https://docs.google.com/spreadsheets/d/1Fj88tEnCeZEJPm-rRVy70YrrVorKvWJzg8IZkPXjTFA/edit#gid=0'

def calc_urls():
    def func(row):
        urls = []
        for f in ('האתר של המחלקה בערבית', 'האתר של המחלקה בעברית'):
            url = row.get(f)
            if url:
                urls.append(f'{url}#{f}')
        row['urls'] = '\n'.join(urls)
    return DF.Flow(
        DF.add_field('urls', 'string'),
        func
    )

if __name__ == '__main__':
    DF.Flow(
        DF.load(url, name='urls'),
        DF.rename_fields({
            'Airtable Code': 'code',
        }),
        calc_urls(),
        DF.select_fields(['code', 'urls']),
        DF.dump_to_path('branch-urls')
    ).process()