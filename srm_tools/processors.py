import dataflows as DF

from dataflows_airtable import AIRTABLE_ID_FIELD

def ensure_fields(field_map, resources=None):
    return [ensure_field(key, args, resources=resources) for key, args in field_map.items()]


def ensure_field(name, args, resources=None):
    args = {'source': args} if isinstance(args, str) else args
    name, source, type, transform = (
        name,
        args.get('source', None),
        args.get('type', 'string'),
        args.get('transform', lambda r: r.get(source) if source else None),
    )
    return DF.add_field(name, type, transform, resources=resources)


def update_mapper():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        row.update(data)

    return func

def fetch_mapper(fields=None):
    def func(rows):
        if rows.res.name == 'current':
            yield from rows
        else:
            for row in rows:
                id = row.pop('id')
                row.pop(AIRTABLE_ID_FIELD, None)
                if fields:
                    for f in fields:
                        row.setdefault(f, None)
                data = row
                yield dict(
                    id=id,
                    data=data,
                )

    return DF.Flow(
        DF.add_field('data', 'object', None, resources=-1),
        func,
        DF.select_fields(['id', 'data'], resources=-1),
    )
