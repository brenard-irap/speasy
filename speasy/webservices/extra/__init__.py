# -*- coding: utf-8 -*-

"""cda package for Space Physics WebServices Client."""

__author__ = """Alexis Jeandet"""
__email__ = 'alexis.jeandet@member.fsf.org'
__version__ = '0.1.0'

import logging
from typing import Optional
from importlib import import_module

from speasy.config import SPEASY_CONFIG_DIR
from speasy.core import AnyDateTimeType, AllowedKwargs
from speasy.core.dataprovider import DataProvider, GET_DATA_ALLOWED_KWARGS
from speasy.core.inventory.indexes import SpeasyIndex, ParameterIndex
from speasy.products.variable import SpeasyVariable
from speasy.core.cache import CACHE_ALLOWED_KWARGS

log = logging.getLogger(__name__)


def user_inventory_dir():
    import os
    return os.path.join(SPEASY_CONFIG_DIR, "extra")


def get_or_make_node(path: str, root: SpeasyIndex) -> SpeasyIndex:
    parts = path.split('/', maxsplit=1)
    name = parts[0]
    if name not in root.__dict__:
        root.__dict__[name] = SpeasyIndex(name=name, provider='extra', uid='')
    if len(parts) == 1:
        return root.__dict__[name]
    return get_or_make_node(parts[1], root.__dict__[name])


def load_inventory_file(file: str, root: SpeasyIndex):
    import yaml
    with open(file, 'r') as f:
        entries = yaml.safe_load(f)
        for name, entry in entries.items():
            path = f"{entry['inventory_path']}/{name}"
            parent = get_or_make_node(entry['inventory_path'], root)
            entry_meta = {"spz_extra_cfg": entry}
            if entry.get('module') and entry.get('method'):
                parameter = ParameterIndex(name=name, provider='extra', uid=path, meta=entry_meta)
                parent.__dict__[parameter.spz_name()] = parameter


class ExtraParameters(DataProvider):
    def __init__(self):
        DataProvider.__init__(self, provider_name='extra', provider_alt_names=['extra_parameters'],
                              inventory_disable_proxy=True)

    def build_inventory(self, root: SpeasyIndex):
        from glob import glob
        for file in glob(f"{user_inventory_dir()}/*.y*ml"):
            load_inventory_file(file, root)
        return root

    def _parameter_index(self, product: str or ParameterIndex) -> ParameterIndex:
        if type(product) is str:
            if product in self.flat_inventory.parameters:
                return self.flat_inventory.parameters[product]
            else:
                raise ValueError(f"Unknown product {product}")
        elif isinstance(product, ParameterIndex):
            return product
        else:
            raise ValueError(f"Got unexpected type {type(product)}, expecting str or ParameterIndex")

    @AllowedKwargs(GET_DATA_ALLOWED_KWARGS + CACHE_ALLOWED_KWARGS + ['additional_arguments'])
    def get_data(self, product: str or ParameterIndex, start_time: AnyDateTimeType, stop_time: AnyDateTimeType,
                 **kwargs) -> Optional[SpeasyVariable]:
        var = self._get_data(product=self._parameter_index(product), start_time=start_time, stop_time=stop_time,
                             **kwargs)
        return var

    def _get_data(self, product: ParameterIndex, start_time: AnyDateTimeType, stop_time: AnyDateTimeType, **kwargs) -> \
        Optional[
            SpeasyVariable]:
        module = import_module(product.spz_extra_cfg.get('module'))
        method = getattr(module, product.spz_extra_cfg.get('method'))
        arguments = product.spz_extra_cfg.get('arguments')
        additional_arguments = kwargs.get('additional_arguments', None)
        if arguments:
            for k in arguments.keys():
                kwargs[k] = arguments[k]
                if additional_arguments and k in additional_arguments:
                    kwargs[k] = additional_arguments[k]
        return method(start_time, stop_time, **kwargs)
