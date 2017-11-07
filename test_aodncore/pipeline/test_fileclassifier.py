#!/usr/bin/env python
"""Unit tests for FileClassifier classes"""

import os
import unittest
from tempfile import mkstemp

from aodncore.pipeline import FileClassifier
from aodncore.pipeline.exceptions import InvalidFileFormatError, InvalidFileNameError, InvalidFileContentError
from test_aodncore.testlib import BaseTestCase, make_test_file


class TestFileClassifier(BaseTestCase):
    def setUp(self):
        tmp_handle, self.testfile = mkstemp(prefix='IMOS_ANMN-NRS_', suffix='.nc')

    def tearDown(self):
        os.remove(self.testfile)

    def test_get_file_name_fields(self):
        fields = ['IMOS', 'ANMN-NRS', 'CDEKOSTUZ', '20121113T001841Z', 'NRSMAI', 'FV01', 'Profile-SBE-19plus']
        filename = '_'.join(fields) + '.nc'
        self.assertEqual(FileClassifier._get_file_name_fields(filename), fields)
        fields = ['IMOS', 'ANMN-NRS', 'ACESTZ', '20140507T000300Z', 'NRSKAI', 'FV02',
                  'NRSKAI-1405-NXIC-CTD-36.12-burst-averaged', 'END-20141028T230300Z', 'C-20160202T020400Z']
        filename = '_'.join(fields) + '.nc'
        self.assertEqual(FileClassifier._get_file_name_fields(filename), fields)
        fields = ['IMOS', 'ANMN-NRS', '20110203', 'NRSPHB', 'FV01', 'LOGSHT']
        filename = '_'.join(fields) + '.nc'
        self.assertEqual(FileClassifier._get_file_name_fields(filename), fields)
        with self.assertRaisesRegexp(InvalidFileNameError, 'has less than 4 fields in file name'):
            FileClassifier._get_file_name_fields('bad_file_name', min_fields=4)

    def test_get_facility(self):
        filename = 'IMOS_ANMN-NRS_CDEKOSTUZ_20121113T001841Z_NRSMAI_FV01_Profile-SBE-19plus.nc'
        self.assertEqual(FileClassifier._get_facility(filename), ('ANMN', 'NRS'))
        with self.assertRaisesRegexp(InvalidFileNameError, 'Missing sub-facility in file name'):
            FileClassifier._get_facility('IMOS_NO_SUB_FACILITY.nc')

    def test_bad_file(self):
        self.assertRaises(InvalidFileFormatError, FileClassifier._get_nc_att, self.testfile, 'attribute')

    def test_get_nc_att(self):
        make_test_file(self.testfile, {'site_code': 'TEST1', 'title': 'Test file'})
        self.assertEqual(FileClassifier._get_nc_att(self.testfile, 'site_code'), 'TEST1')
        self.assertEqual(FileClassifier._get_nc_att(self.testfile, 'missing', ''), '')
        self.assertEqual(FileClassifier._get_nc_att(self.testfile, ['site_code', 'title']),
                         ['TEST1', 'Test file'])
        self.assertRaises(InvalidFileContentError, FileClassifier._get_nc_att, self.testfile, 'missing')

    def test_get_site_code(self):
        make_test_file(self.testfile, {'site_code': 'TEST1'})
        self.assertEqual(FileClassifier._get_site_code(self.testfile), 'TEST1')

    def test_get_variable_names(self):
        make_test_file(self.testfile, {}, PRES={}, TEMP={}, PSAL={})
        output = set(FileClassifier._get_variable_names(self.testfile))
        self.assertEqual(output, {'PRES', 'TEMP', 'PSAL'})

    def test_make_path(self):
        path = FileClassifier._make_path(['dir1', u'dir2', u'dir3'])
        self.assertTrue(isinstance(path, str))


if __name__ == '__main__':
    unittest.main()
