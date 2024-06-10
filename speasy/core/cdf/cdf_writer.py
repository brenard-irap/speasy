import io
from typing import List, Union, Optional, Mapping

import numpy as np

from speasy import SpeasyVariable
from speasy.core.data_containers import VariableAxis
import pycdfpp
import re

_PTR_rx = re.compile(".*_PTR_\d+")


def _simplify_shape(values: np.ndarray) -> np.ndarray:
    if len(values.shape) == 2 and values.shape[1] == 1:
        return np.reshape(values, (-1))
    return values


def _convert_attributes_to_variables(variable_name: str, attrs: Mapping, cdf: pycdfpp.CDF):
    clean_attrs = {}
    for name, attr_v in attrs.items():
        target_name = f"{variable_name}_{name}_{variable_name}"
        if _PTR_rx.match(name):
            cdf.add_variable(
                name=target_name,
                values=attr_v
            )
            clean_attrs[name] = target_name
        else:
            clean_attrs[name] = attr_v
    return clean_attrs


def _write_axis(ax: VariableAxis, cdf: pycdfpp.CDF, compress_variables=False) -> bool:
    data_type = None
    if ax.values.dtype == np.dtype("datetime64[ns]"):
        data_type = pycdfpp.DataType.CDF_TIME_TT2000
    cdf.add_variable(
        name=ax.name,
        values=_simplify_shape(ax.values),
        data_type=data_type,
        compression=pycdfpp.CompressionType.gzip_compression if compress_variables else pycdfpp.CompressionType.no_compression
    )
    return True


def _write_variable(v: SpeasyVariable, cdf: pycdfpp.CDF, already_saved_axes: List[VariableAxis],
                    compress_variables=False) -> bool:
    def _already_in_cdf(ax: VariableAxis):
        for a in already_saved_axes:
            if a == ax:
                return a.name
        else:
            return None

    depends = {}
    for index, ax in enumerate(v.axes):
        a = _already_in_cdf(ax)
        if a is None:
            _write_axis(ax, cdf, compress_variables)
            depends[f"DEPEND_{index}"] = ax.name
            already_saved_axes.append(ax)
        else:
            depends[f"DEPEND_{index}"] = a.name

    attributes = v.meta
    attributes.update(depends)
    cdf.add_variable(
        name=v.name,
        values=_simplify_shape(v.values),
        attributes=_convert_attributes_to_variables(variable_name=v.name, attrs=attributes, cdf=cdf),
        compression=pycdfpp.CompressionType.gzip_compression if compress_variables else pycdfpp.CompressionType.no_compression
    )


def save_variables(variables: List[SpeasyVariable], file: Optional[str] = None, compress_variables=False) -> Union[
    bool, bytes]:
    axes = []
    cdf = pycdfpp.CDF()

    for variable in variables:
        if not isinstance(variable, SpeasyVariable):
            raise ValueError(f"Expected SpeasyVariable, got {type(variable)}")
        _write_variable(variable, cdf, axes, compress_variables)
    if file:
        return pycdfpp.save(cdf, file)
    else:
        return pycdfpp.save(cdf)
