import logging
from typing import List, Optional

import pyistp
from pyistp.loader import DataVariable, ISTPLoader

from speasy.core.file_access import urlopen_with_retry
from speasy.core.inventory.indexes import ParameterIndex, DatasetIndex

log = logging.getLogger(__name__)


def filter_variable_meta(datavar: DataVariable) -> dict:
    keep_list = ['CATDESC', 'FIELDNAM', 'UNITS', 'UNIT_PTR', 'DISPLAY_TYPE', 'LABLAXIS', 'LABL_PTR_1', 'LABL_PTR_2',
                 'LABL_PTR_3']
    base = {key: value for key, value in datavar.attributes.items() if key in keep_list}
    if len(datavar.values.shape) == 1:
        base['spz_shape'] = 1
    else:
        base['spz_shape'] = datavar.values.shape[1:]
    return base


def filter_dataset_meta(dataset: ISTPLoader) -> dict:
    keep_list = ['Caveats', 'Rules_of_use']
    return {key: dataset.attribute(key) for key in dataset.attributes() if key in keep_list}


def extract_parameter(cdf: ISTPLoader, var_name: str, provider: str, uid_fmt: str = "{var_name}", meta=None) -> \
    Optional[ParameterIndex]:
    try:
        datavar = cdf.data_variable(var_name)
        meta = meta or {}
        if datavar is not None:
            return ParameterIndex(name=var_name, provider=provider, uid=uid_fmt.format(var_name=var_name),
                                  meta={**filter_variable_meta(datavar), **meta})
    except IndexError or RuntimeError:
        print(f"Issue loading {var_name} from {cdf}")

    return None


def _extract_parameters_impl(cdf: ISTPLoader, provider: str, uid_fmt: str = "{var_name}", meta=None) -> List[
    ParameterIndex]:
    return list(filter(lambda p: p is not None,
                       map(lambda var_name: extract_parameter(cdf, var_name, provider, uid_fmt, meta=meta),
                           cdf.data_variables())))


def extract_parameters(url: str, provider: str, uid_fmt: str = "{var_name}", meta=None) -> List[ParameterIndex]:
    indexes: List[ParameterIndex] = []
    try:
        with urlopen_with_retry(url) as remote_cdf:
            cdf = pyistp.load(buffer=remote_cdf.read())
            return _extract_parameters_impl(cdf, provider=provider, uid_fmt=uid_fmt, meta=meta)

    except RuntimeError:
        print(f"Issue loading {url}")
    return indexes


def make_dataset_index(url: str, name: str, provider: str, uid: str, meta=None,
                       params_uid_format: str = "{var_name}", params_meta=None) -> Optional[DatasetIndex]:
    try:
        with urlopen_with_retry(url) as remote_cdf:
            meta = meta or {}
            params_meta = params_meta or {}
            cdf = pyistp.load(buffer=remote_cdf.read())
            dataset = DatasetIndex(name=name, provider=provider, uid=uid, meta={**filter_dataset_meta(cdf), **meta})
            dataset.__dict__.update(
                {p.spz_name(): p for p in
                 _extract_parameters_impl(cdf, provider=provider, uid_fmt=params_uid_format, meta=params_meta)})
            return dataset
    except RuntimeError:
        print(f"Issue loading {url}")
    return None