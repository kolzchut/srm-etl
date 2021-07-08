import json
import codecs
import pprint

import requests

import dataflows as DF

KEEP_FIELDS = ['cat', 'name']
DT_SUFFIXES = dict((k, i) for i, k in enumerate(['', 'i', 'ss', 't', 's', 'base64', 'f', 'is']))

def scrape_click():
    print('hi')
    try:
        docs = json.load(open('click-cache.json'))
    except:
        docs = requests.get('https://clickrevaha-sys.molsa.gov.il/api/solr?rows=1000').json().get('response').get('docs')
        json.dump(docs, open('click-cache.json', 'w'))

    print(len(docs))
    all_keys = set()
    for doc in docs:
        all_keys.update(k for k, v in doc.items() if v)
    config = dict()
    for k in all_keys:
        if k in KEEP_FIELDS:
            config[k] = [[k, k, '']]
        else:
            suffix = k.split('_')[-1]
            if suffix in DT_SUFFIXES:
                prefix = k[:-len(suffix)-1]
                config.setdefault(prefix, []).append((prefix, k, suffix))
    concat_fields = dict()
    for k, suffixes in config.items():
        prefix, k, _ = sorted(suffixes, key=lambda s: DT_SUFFIXES[s[2]])[0]
        prefix = prefix.lower()
        concat_fields[prefix] = [k] if prefix != k else []

    docs = (
        dict((k, doc.get(k)) for k in all_keys)
        for doc in docs
    )
    
    return DF.Flow(
        docs,
        DF.concatenate(concat_fields),
    )
    # all_keys = all_keys - set(base64_keys)
    # print(all_keys)
    # print(base64_keys)
    # for doc in docs:
    #     ret = dict((k, doc.get(k)) for k in all_keys)
    #     ret.update(dict(
    #         (k[:-7],
    #          codecs.decode(doc.get(k).encode('ascii'), 'base64').decode('utf8')
    #          if doc.get(k)
    #          else None
    #         ) for k in base64_keys)
    #     )
    #     ret = dict((k, ret[k]) for k in sorted(ret.keys()))
    #     yield ret

def remove_nbsp():
    def func(row):
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.replace('&nbsp;', ' ')
    return func

# def keep_prominent():
#     def func(rows):
#         counts = dict()
#         for row in rows:
#             for k, v in row.items():
#                 if v:
#                     counts.setdefault(k, set())
#                     counts[k].add(json.dumps(v))
#             yield row
#         counts = dict((k, len(v)) for k, v in counts.items())
#         pprint.pprint(sorted(counts.items(), key=lambda x: -x[1]))
#     return func

def operator(*_):
    DF.Flow(
        scrape_click(),
        remove_nbsp(),
        # keep_prominent(),
        DF.update_resource(None, path='data/click.csv'),
        DF.dump_to_path('click'),
    ).process()


if __name__ == '__main__':
    operator()