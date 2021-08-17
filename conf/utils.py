import os
import sys
from enum import Enum


ENVVAR_ARRAY_SEPARATOR = ','
ENVVAR_MAP_KV_SEPARATOR = '='
ENVVAR_BOOLS = {'true': True, 'false': False}


class EnvVarConfigException(Exception):
    ...


class EnvVarStrategyException(Exception):
    ...


class EnvVarStrategy(Enum):
    ITEM = 'single value'
    ARRAY = 'array of values'
    MAP = 'map of keys/values'


def get_env(value, default='', strategy=EnvVarStrategy.ITEM, required=True):
    if strategy == EnvVarStrategy.ITEM:
        transformed_value = get_env_item(value, default)
    elif strategy == EnvVarStrategy.ARRAY:
        transformed_value = get_env_array(value, default)
    elif strategy == EnvVarStrategy.MAP:
        transformed_value = get_env_map(value, default)
    else:
        raise EnvVarStrategyException(
            f'{strategy}: Unknown environment variable strategy.'
        )
    if required is True and not transformed_value:
        raise EnvVarConfigException(f'{value} is a required environment variable.')
    return transformed_value


def _clean_env(value):
    value = value.strip()
    if value in ENVVAR_BOOLS.keys():
        clean = ENVVAR_BOOLS[value]
    else:
        clean = value
    return clean


def get_env_item(value, default):
    return _clean_env(os.getenv(value, default))


def get_env_array(value, default):
    return [
        _clean_env(part)
        for part in os.getenv(value, default).split(ENVVAR_ARRAY_SEPARATOR)
    ]


def get_env_map(value, default):
    raise NotImplementedError
