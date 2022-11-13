from dataflows.helpers import ResourceMatcher


def unwind(
    from_key, to_key, to_key_type='string',
    transformer=None, resources=None, source_delete=True, allow_empty=None
):

    """From a row of data, generate a row per value from from_key, where the value is set onto to_key."""
    from dataflows.processors.add_computed_field import get_new_fields

    def _unwinder(rows):
        for row in rows:
            try:
                values = row[from_key]
                iter(values)
                if allow_empty and len(values) == 0:
                    values = [None]
                for value in values:
                    ret = {}
                    ret.update(row)
                    ret[to_key] = value
                    if source_delete is True:
                        del ret[from_key]
                    yield ret
            except TypeError:
                # no iterable to unwind. Take the value we have and set it on the to_key.
                ret = {}
                ret.update(row)
                ret[to_key] = ret[from_key] if transformer is None else transformer(ret[from_key])
                if source_delete is True:
                    del ret[from_key]
                yield ret

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                new_fields = get_new_fields(
                    resource, [{'target': {'name': to_key, 'type': to_key_type}}]
                )
                resource['schema']['fields'] = [
                    field
                    for field in resource['schema']['fields']
                    if not source_delete or not field['name'] == from_key
                ]
                resource['schema']['fields'].extend(new_fields)

        yield package.pkg

        for resource in package:
            if matcher.match(resource.res.name):
                yield _unwinder(resource)
            else:
                yield resource

    return func

