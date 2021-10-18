import dataflows as DF


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
        row.update({k: v for k, v in row.get('data').items()})

    return func
