#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `SpeasyVariable` CDF interfaces."""

import unittest
import os
import astropy.table
import astropy.units
import numpy as np
import pandas as pds
from ddt import data, ddt, unpack

from speasy.core import epoch_to_datetime64
from speasy.core.cdf import _load_variable, load_variable
from speasy.core.cdf.cdf_writer import save_variables
from speasy.products.variable import (DataContainer, SpeasyVariable,
                                      VariableAxis, VariableTimeAxis,
                                      from_dataframe, from_dictionary, merge,
                                      to_dataframe, to_dictionary)

_HERE_ = os.path.dirname(os.path.abspath(__file__))


class CDFLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v = load_variable("BGSEc", f"{_HERE_}/resources/ac_k2_mfi_20220101_v03.cdf")

    def test_variable_is_loaded(self):
        self.assertIsNotNone(self.v)

    def test_variable_shape(self):
        self.assertEqual(self.v.values.shape, (24, 3))


class CDFWriter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.v = _load_variable("BGSEc", buffer=save_variables(
            [load_variable("BGSEc", f"{_HERE_}/resources/ac_k2_mfi_20220101_v03.cdf")]))

    def test_variable_is_loaded(self):
        self.assertIsNotNone(self.v)

    def test_variable_shape(self):
        self.assertEqual(self.v.values.shape, (24, 3))
