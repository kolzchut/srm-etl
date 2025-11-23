URL_SCHEMA = {
    'es:itemType': 'object',
    'es:index': False,
    'es:schema': {
        'fields': [
            {'type': 'string', 'name': 'href'},
            {'type': 'string', 'name': 'text'},
        ]
    },
}

TAXONOMY_ITEM_SCHEMA = {
    'es:itemType': 'object',
    'es:schema': {
        'fields': [
            {'type': 'string', 'name': 'id', 'es:keyword': True},
            {'type': 'string', 'name': 'name', 'es:title': True},
            {'type': 'string', 'name': 'synonyms', 'es:title': True},
        ]
    },
}

ADDRESS_PARTS_SCHEMA = {
    'es:schema': {
        'fields': [
            {'name': 'primary',  'type': 'string', 'es:fields': { 'keyword': { 'type': 'keyword' }}}, 
            {'name': 'secondary', 'type': 'string'}
        ]
    }
}

NON_INDEXED_ADDRESS_PARTS_SCHEMA = {
    'es:schema': {
        'fields': [
            {'name': 'primary', 'type': 'string'}, 
            {'name': 'secondary', 'type': 'string'}
        ]
    }
}

NON_INDEXED_STRING = {'es:itemType': 'string', 'es:index': False}
KEYWORD_STRING = {'es:itemType': 'string', 'es:keyword': True}
KEYWORD_ONLY = {'es:keyword': True}
ITEM_TYPE_STRING = {'es:itemType': 'string'}
ITEM_TYPE_NUMBER = {'es:itemType': 'number'}
AUTOCOMPLETE_STRING = {'es:autocomplete': True}
NO_SCHEMA = {}

LAST_MODIFIED_DATE = {
    'es:itemType': 'date',
    'es:format': 'strict_date_optional_time||epoch_millis'
}
