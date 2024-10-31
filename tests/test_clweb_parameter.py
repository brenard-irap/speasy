#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `clweb` package parameter getting functions."""

import unittest
from ddt import data, ddt
from datetime import datetime, timedelta

import numpy as np

import speasy as spz
from speasy.core import make_utc_datetime


class ParameterRequests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.start = datetime(2000, 1, 1, 1, 1)
        self.stop = datetime(2000, 1, 1, 1, 2)
        self.data = spz.clweb.get_parameter(
            "AC_EPHEMERIS_ORBIT_H0_MFI(13,14,15)", self.start, self.stop, disable_proxy=True, disable_cache=True)
        self.dataset = spz.clweb.get_dataset(
            "ACE:EPHEMERIS:ORBIT_H0_MFI", self.start, self.stop, disable_proxy=True, disable_cache=True)

    @classmethod
    def tearDownClass(self):
        pass

    def test_data_not_none(self):
        self.assertIsNotNone(self.data)

    def test_data_not_empty(self):
        self.assertTrue(len(self.data.values.shape) > 0)

    def test_time_not_empty(self):
        self.assertTrue(len(self.data.time.shape) > 0)

    def test_data_time_compatibility(self):
        self.assertTrue(self.data.values.shape[0] == self.data.time.shape[0])

    def test_time_datatype(self):
        self.assertTrue(self.data.time.dtype == np.dtype('datetime64[ns]'))

    def test_time_range(self):
        min_dt = min(self.data.time[1:] - self.data.time[:-1])
        start = np.datetime64(self.start, 'ns')
        stop = np.datetime64(self.stop, 'ns')
        self.assertTrue(
            start <= self.data.time[0] < (start + min_dt))
        self.assertTrue(
            stop > self.data.time[-1] >= (stop - min_dt))

    def test_dataset_not_none(self):
        self.assertIsNotNone(self.dataset)

    def test_dataset_type(self):
        self.assertTrue(isinstance(self.dataset, spz.Dataset))

    def test_dataset_not_empty(self):
        self.assertTrue(len(self.dataset) > 0)

    def test_dataset_items_datatype(self):
        for item in self.dataset:
            self.assertTrue(isinstance(self.dataset[item], spz.SpeasyVariable))


@ddt
class CLWebParametersPlots(unittest.TestCase):
    def setUp(self):
        try:
            import matplotlib.pyplot as plt
        except ImportError:
            self.skipTest("Can't import matplotlib")

    @data(
        spz.inventories.tree.clweb.Parameters.ACE.ephemeris.orbit_from_H0_MFI.ACE_sc_in_GSE_coord,
        spz.inventories.tree.clweb.Parameters.CLUSTER1.ephemeris.orbit_1mn_resolution.distance,
        spz.inventories.tree.clweb.Parameters.THEMISA.ephemeris.orbit.Position_GSE_dynamically_computed_by_cl
    )
    def test_parameter_line_plot(self, parameter):
        values: spz.SpeasyVariable = spz.get_data(parameter, "2018-01-01", "2018-01-01T01")
        import matplotlib.pyplot as plt
        plt.close('all')
        ax = values.plot()
        self.assertIsNotNone(ax)
        self.assertEqual(len(ax.lines), values.values.shape[1],
                         "Number of lines in the plot should be equal to the number of columns in the data")
        self.assertIn(values.unit, ax.get_ylabel(), "Units should be in the Y axis label")
        for i, name in enumerate(values.columns):
            self.assertIn(name, ax.get_legend().texts[i].get_text(), "Legend should contain the column names")

    @data(
        spz.inventories.tree.csa.Cluster.Cluster_1.CIS_HIA1.C1_CP_CIS_HIA_HS_1D_PEF.flux__C1_CP_CIS_HIA_HS_1D_PEF,
        #spz.inventories.tree.clweb.Parameters.MAVEN.STATIC.mavpds_sta_c6.mav_sta_c6_energy
    )
    def test_parameter_colormap_lot(self, parameter):
        values: spz.SpeasyVariable = spz.get_data(parameter, "2018-01-01", "2018-01-01T01")
        import matplotlib.pyplot as plt
        plt.close('all')
        ax = values.plot()
        self.assertIsNotNone(ax)
        self.assertIn(values.axes[1].unit, ax.get_ylabel(), "Units should be in the Y axis label")


if __name__ == '__main__':
    unittest.main()
