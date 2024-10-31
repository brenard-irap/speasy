#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `clweb` package."""
import os
import unittest
from datetime import datetime, timezone

import numpy as np
import speasy as spz
from ddt import data, ddt, unpack
from speasy.config import clweb as clweb_cfg
from speasy.inventories import flat_inventories
from speasy.products import SpeasyVariable
from speasy.core.impex import ImpexProductType
from speasy.core.impex.exceptions import MissingCredentials, UnavailableEndpoint
from speasy.core.impex.parser import ImpexXMLParser, to_xmlid

_HERE_ = os.path.dirname(os.path.abspath(__file__))


def has_clweb_creds() -> bool:
    return spz.config.clweb.username() != "" and spz.config.clweb.password() != ""


class UserProductsRequestsWithoutCreds(unittest.TestCase):
    def setUp(self):
        os.environ[spz.config.clweb.username.env_var_name] = ""
        os.environ[spz.config.clweb.password.env_var_name] = ""

    def tearDown(self):
        os.environ.pop(spz.config.clweb.username.env_var_name)
        os.environ.pop(spz.config.clweb.password.env_var_name)

    def test_get_user_timetables(self):
        with self.assertRaises(MissingCredentials):
            spz.clweb.get_user_timetable("Id doesn't matter")

    def test_get_user_parameters(self):
        with self.assertRaises(MissingCredentials):
            spz.clweb.get_user_parameter("Id doesn't matter", start_time="2016-06-01", stop_time="2016-06-01T12:00:00")

    def test_get_user_catalogs(self):
        with self.assertRaises(MissingCredentials):
            spz.clweb.get_user_catalog("Id doesn't matter")


class PublicProductsRequests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_get_variable(self):
        start_date = datetime(2006, 1, 8, 1, 0, 0, tzinfo=timezone.utc)
        stop_date = datetime(2006, 1, 8, 1, 0, 10, tzinfo=timezone.utc)
        parameter_id = "C1_FGM_4SEC(6,7,8,14)"
        result = spz.clweb.get_parameter(parameter_id, start_date, stop_date, disable_proxy=True, disable_cache=True)
        self.assertIsNotNone(result)
        start_date = datetime(2016, 1, 8, 1, 0, 0, tzinfo=timezone.utc)
        stop_date = datetime(2016, 1, 8, 1, 0, 10, tzinfo=timezone.utc)
        parameter_id = "C1_HIA_MOMENTS(20)"
        result = spz.clweb.get_parameter(parameter_id, start_date, stop_date, disable_proxy=True, disable_cache=True)
        self.assertIsNotNone(result)

    def test_get_variable_over_midnight(self):
        start_date = datetime(2006, 1, 8, 23, 30, 0, tzinfo=timezone.utc)
        stop_date = datetime(2006, 1, 9, 0, 30, 0, tzinfo=timezone.utc)
        parameter_id = "C1_FGM_4SEC(6,7,8,14)"
        result = spz.clweb.get_parameter(parameter_id, start_date, stop_date, disable_proxy=True, disable_cache=True)
        self.assertIsNotNone(result)

    def test_returns_none_for_a_request_outside_of_range(self):
        with self.assertLogs('speasy.core.dataprovider', level='WARNING') as cm:
            start_date = datetime(1999, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            stop_date = datetime(1999, 1, 30, 0, 0, 0, tzinfo=timezone.utc)
            parameter_id = "THA_EPHEMERIS_ORBIT(1,2,3)"
            result = spz.clweb.get_parameter(parameter_id, start_date, stop_date,
                                             disable_proxy=True, disable_cache=True)
            self.assertIsNone(result)
            self.assertTrue(
                any(["outside of its definition range" in line for line in cm.output]))

    def test_get_product_range(self):
        param_range = spz.clweb.parameter_range(spz.clweb.list_parameters()[0])
        self.assertIsNotNone(param_range)
        dataset_range = spz.clweb.dataset_range(spz.clweb.list_datasets()[0])
        self.assertIsNotNone(dataset_range)

    def test_list_parameters(self):
        result = spz.clweb.list_parameters()
        self.assertTrue(len(result) != 0)

    def test_get_parameter(self):
        start, stop = datetime(2000, 1, 1), datetime(2000, 1, 2)
        r = spz.clweb.get_parameter("AC_EPHEMERIS_ORBIT_H0_MFI(13,14,15)", start, stop, disable_cache=True)
        #self.assertEqual(r.name, "imf")
        #self.assertEqual(r.columns, ['bx', 'by', 'bz'])
        self.assertEqual(r.unit, "km")
        self.assertIsNotNone(r)

    def test_list_datasets(self):
        result = spz.clweb.list_datasets()
        self.assertTrue(len(result) != 0)

    def test_get_dataset(self):
        start, stop = datetime(2012, 1, 1), datetime(2012, 1, 1, 1)
        r = spz.clweb.get_dataset("THEMISA:EPHEMERIS:ORBIT", start, stop, disable_cache=True)
        self.assertTrue(len(r) != 0)

    def test_list_timetables(self):
        result = spz.clweb.list_timetables()
        self.assertTrue(len(result) != 0)

    def test_get_sharedtimeTable_0(self):
        r = spz.clweb.get_timetable("test1.xml")
        self.assertIsNotNone(r)

    def test_get_timetable_from_Index(self):
        r = spz.clweb.get_timetable(spz.clweb.list_timetables()[-1])
        self.assertIsNotNone(r)

    def test_get_multidimensional_data(self):
        r = spz.clweb.get_data("WI_3DP_PLSP_SPECTRO_TIME_ENERGY(4)", "2021-07-30T00:00:00", "2021-07-30T00:05:00")
        self.assertIsNotNone(r)
        self.assertIsNotNone(r.values)


class PrivateProductsRequests(unittest.TestCase):
    def setUp(self):
        if not has_clweb_creds():
            self.skipTest("Missing CLWeb_Webservice credentials")

    def tearDown(self):
        pass

    def test_list_user_timetables(self):
        result = spz.clweb.list_user_timetables()
        self.assertTrue(len(result) != 0)

    def test_get_user_timetables(self):
        result = spz.clweb.get_user_timetable(spz.clweb.list_user_timetables()[0])
        self.assertIsNotNone(result)
        self.assertTrue(len(result) != 0)


@ddt
class CLWebModule(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_load_obs_datatree(self):
        with open(os.path.normpath(f'{_HERE_}/resources/clweb_obsdatatree.xml')) as obs_xml:
            flat_inventories.clweb.parameters.clear()
            flat_inventories.clweb.datasets.clear()
            root = ImpexXMLParser.parse(obs_xml.read(), is_public=True, provider_name='clweb')
            flat_inventories.clweb.update(root)
            self.assertIsNotNone(root)
            # grep -o -i '<parameter ' clweb_obsdatatree.xml | wc -l
            # MEX_EPHEMERIS_ORBIT(5) and MEX_EPHEMERIS_ORBIT(9) have the same name into the same dataset
            # WI_SWE_H1(35) and WI_SWE_H1(37) have the same name into the same dataset
            # => 2170 - 2 = 2168 products
            self.assertEqual(len(spz.clweb.list_parameters()), 2168)
            # grep -o -i '<dataset ' clweb_obsdatatree.xml | wc -l
            # PSP:SPC:L2_A_CURRENT
            # PSP:SPC:L2_B_CURRENT
            # PSP:SPC:L2_C_CURRENT
            # PSP:SPC:L2_D_CURRENT
            # PSP:SPC:L2_DIFF_CHARGE
            # PSP:SPC:MOM
            # PSP:SPI:AF00MOM
            # PSP:SPI:AF00_SPECTRO_E
            # PSP:SPI:AF01MOM
            # PSP:SPI:AF01_SPECTRO_E
            # PSP:SPI:SF00MOM
            # PSP:SPI:SF00_SPECTRO_E
            # PSP:SPI:SF01MOM
            # PSP:SPI:SF01_SPECTRO_E
            self.assertEqual(len(spz.clweb.list_datasets()), 363)
            for d in spz.clweb.list_datasets():
                d.spz_uid().isidentifier()
        spz.update_inventories()

    @data(
        (spz.clweb.list_timetables()[-1], ImpexProductType.TIMETABLE),
        (to_xmlid(spz.clweb.list_timetables()[-1]), ImpexProductType.TIMETABLE),
        (spz.clweb.list_datasets()[-1], ImpexProductType.DATASET),
        (to_xmlid(spz.clweb.list_datasets()[-1]), ImpexProductType.DATASET),
        (spz.inventories.data_tree.clweb.Parameters.THEMISA.ephemeris.orbit.Position_GEI.x,
         ImpexProductType.COMPONENT),
        (to_xmlid(spz.inventories.data_tree.clweb.Parameters.THEMISA.ephemeris.orbit.Position_GEI.x),
         ImpexProductType.COMPONENT),
        ('this xml id is unlikely to exist', ImpexProductType.UNKNOWN),
        (spz.inventories.data_tree.clweb.Parameters.ACE, ImpexProductType.UNKNOWN)
    )
    @unpack
    def test_returns_product_type_from_either_id_or_index(self, index, expexted_type):
        result_type = spz.clweb.product_type(index)
        self.assertEqual(result_type, expexted_type)

    @data({'sampling': '1'},
          {'unknown_arg': 10})
    def test_raises_if_user_passes_unexpected_kwargs_to_get_data(self, kwargs):
        with self.assertRaises(TypeError):
            spz.get_data('clweb/C1_FGM_4SEC(6,7,8,14)', "2018-01-01", "2018-01-02", **kwargs)
        with self.assertRaises(TypeError):
            spz.clweb.get_data('C1_FGM_4SEC(6,7,8,14)', "2018-01-01", "2018-01-02", **kwargs)

    def test_raises_if_user_passes_unknown_product_kwargs_to_get_data(self):
        with self.assertRaises(ValueError):
            spz.get_data('clweb/This_product_does_not_exist')
        with self.assertRaises(ValueError):
            spz.get_data('clweb/This_product_does_not_exist', "2018-01-01", "2018-01-02")

    #def test_non_regression_CDF_ISTP_with_proxy_and_config(self):
    #    ref = spz.get_data(spz.inventories.tree.clweb.Parameters.MMS.MMS1.FPI.fast_mode.mms1_fpi_dismoms.mms1_dis_omni,
    #                       "2021-06-01", "2021-06-08T02", output_format='CDF_ISTP')
    #    os.environ[amda_cfg.output_format.env_var_name] = 'CDF_ISTP'
    #    var = spz.get_data(spz.inventories.tree.clweb.Parameters.MMS.MMS1.FPI.fast_mode.mms1_fpi_dismoms.mms1_dis_omni,
    #                       "2021-06-01", "2021-06-08T02")
    #    self.assertTrue(len(ref.axes), 2)
    #    self.assertTrue(len(var.axes), 2)
    #    self.assertTrue(np.all(var.axes[1].values == ref.axes[1].values))


if __name__ == '__main__':
    unittest.main()
