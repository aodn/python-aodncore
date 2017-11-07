import os
from uuid import uuid4

from aodncore.pipeline.steps.resolve import (get_resolve_runner, DirManifestResolveRunner, MapManifestResolveRunner,
                                             RsyncManifestResolveRunner, SimpleManifestResolveRunner,
                                             SingleFileResolveRunner, ZipFileResolveRunner)
from test_aodncore.testlib import MOCK_LOGGER, BaseTestCase

TESTDATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'testdata')
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
BAD_ZIP = os.path.join(TESTDATA_DIR, 'bad.zip')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
GOOD_ZIP = os.path.join(TESTDATA_DIR, 'good.zip')
RECURSIVE_ZIP = os.path.join(TESTDATA_DIR, 'recursive.zip')
INVALID_FILE = os.path.join(TESTDATA_DIR, 'invalid.png')
NOT_NETCDF_NC_FILE = os.path.join(TESTDATA_DIR, 'not_a_netcdf_file.nc')
TEST_MANIFEST_NC = os.path.join(TESTDATA_DIR, 'test_manifest.nc')
TEST_DIR_MANIFEST_NC = os.path.join(TESTDATA_DIR, 'layer1', 'layer2', 'test_manifest.nc')
DIR_MANIFEST = os.path.join(TESTDATA_DIR, 'test.dir_manifest')
MAP_MANIFEST = os.path.join(TESTDATA_DIR, 'test.map_manifest')
RSYNC_MANIFEST = os.path.join(TESTDATA_DIR, 'test.rsync_manifest')
SIMPLE_MANIFEST = os.path.join(TESTDATA_DIR, 'test.manifest')


class MockConfig(object):
    pipeline_config = {
        'global': {
            'wip_dir': TESTDATA_DIR
        }
    }


MOCK_CONFIG = MockConfig


class TestPipelineStepsResolve(BaseTestCase):
    def test_get_resolve_runner(self):
        map_manifest_resolve_runner = get_resolve_runner(MAP_MANIFEST, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        self.assertIsInstance(map_manifest_resolve_runner, MapManifestResolveRunner)

        rsync_manifest_resolve_runner = get_resolve_runner(RSYNC_MANIFEST, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        self.assertIsInstance(rsync_manifest_resolve_runner, RsyncManifestResolveRunner)

        simple_manifest_resolve_runner = get_resolve_runner(SIMPLE_MANIFEST, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        self.assertIsInstance(simple_manifest_resolve_runner, SimpleManifestResolveRunner)

        nc_resolve_runner = get_resolve_runner(GOOD_NC, self.temp_dir, TESTDATA_DIR, MOCK_CONFIG, MOCK_LOGGER)
        self.assertIsInstance(nc_resolve_runner, SingleFileResolveRunner)

        unknown_file_extension = get_resolve_runner(str(uuid4()), self.temp_dir, TESTDATA_DIR, MOCK_CONFIG,
                                                    MOCK_LOGGER)
        self.assertIsInstance(unknown_file_extension, SingleFileResolveRunner)

        zip_resolve_runner = get_resolve_runner(GOOD_ZIP, self.temp_dir, TESTDATA_DIR, MOCK_CONFIG, None)
        self.assertIsInstance(zip_resolve_runner, ZipFileResolveRunner)


class TestDirManifestResolveRunner(BaseTestCase):
    def test_dir_manifest_resolve_runner(self):
        dir_manifest_resolve_runner = DirManifestResolveRunner(DIR_MANIFEST, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        collection = dir_manifest_resolve_runner.run()

        self.assertEqual(collection[0].src_path, os.path.join(MOCK_CONFIG.pipeline_config['global']['wip_dir'],
                                                              'layer1', 'layer2',
                                                              os.path.basename(TEST_DIR_MANIFEST_NC)))
        self.assertEqual(collection[1].src_path, os.path.join(MOCK_CONFIG.pipeline_config['global']['wip_dir'],
                                                              os.path.basename(NOT_NETCDF_NC_FILE)))


class TestMapManifestResolveRunner(BaseTestCase):
    def test_map_manifest_resolve_runner(self):
        map_manifest_resolve_runner = MapManifestResolveRunner(MAP_MANIFEST, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        collection = map_manifest_resolve_runner.run()

        self.assertEqual(collection[0].src_path, os.path.join(MOCK_CONFIG.pipeline_config['global']['wip_dir'],
                                                              os.path.basename(TEST_MANIFEST_NC)))

        self.assertEqual(collection[0].dest_path, 'UNITTEST/NOT/A/REAL/PATH')


class TestRsyncManifestResolveRunner(BaseTestCase):
    def test_rsync_manifest_resolve_runner(self):
        rsync_manifest_resolve_runner = RsyncManifestResolveRunner(RSYNC_MANIFEST, self.temp_dir, MOCK_CONFIG,
                                                                   MOCK_LOGGER)
        collection = rsync_manifest_resolve_runner.run()

        self.assertEqual(len(collection), 2)
        self.assertFalse(collection[0].is_deletion)
        self.assertTrue(collection[1].is_deletion)

        self.assertEqual(collection[0].src_path, os.path.join(MOCK_CONFIG.pipeline_config['global']['wip_dir'],
                                                              os.path.basename(TEST_MANIFEST_NC)))

        self.assertEqual(collection[1].src_path, os.path.join(TESTDATA_DIR, 'aoml/1900728/1900728_Rtraj.nc'))


class TestSimpleManifestResolveRunner(BaseTestCase):
    def test_simple_manifest_resolve_runner(self):
        simple_manifest_resolve_runner = SimpleManifestResolveRunner(SIMPLE_MANIFEST, self.temp_dir, MOCK_CONFIG,
                                                                     MOCK_LOGGER)
        collection = simple_manifest_resolve_runner.run()

        self.assertEqual(collection[0].src_path, os.path.join(MOCK_CONFIG.pipeline_config['global']['wip_dir'],
                                                              os.path.basename(TEST_MANIFEST_NC)))


class TestSingleFileResolveRunner(BaseTestCase):
    def test_single_file_resolve_runner(self):
        single_file_resolve_runner = SingleFileResolveRunner(GOOD_NC, self.temp_dir, MOCK_CONFIG, MOCK_LOGGER)
        collection = single_file_resolve_runner.run()

        good_nc = os.path.join(self.temp_dir, os.path.basename(GOOD_NC))

        self.assertEqual(len(collection), 1)

        self.assertTrue(os.path.exists(good_nc))
        self.assertEqual(collection[0].src_path, good_nc)


class TestZipFileResolveRunner(BaseTestCase):
    def test_zip_file_resolve_runner(self):
        collection_dir = os.path.join(self.temp_dir, 'collection')
        zip_file_resolve_runner = ZipFileResolveRunner(BAD_ZIP, collection_dir, MOCK_CONFIG, MOCK_LOGGER)
        collection = zip_file_resolve_runner.run()

        good_nc = os.path.join(collection_dir, os.path.basename(GOOD_NC))
        bad_nc = os.path.join(collection_dir, os.path.basename(BAD_NC))

        self.assertEqual(len(collection), 2)

        self.assertEqual(collection[0].src_path, bad_nc)
        self.assertTrue(os.path.exists(bad_nc))

        self.assertEqual(collection[1].src_path, good_nc)
        self.assertTrue(os.path.exists(good_nc))

    def test_recursive_zip(self):
        collection_dir = os.path.join(self.temp_dir, 'collection')
        zip_file_resolve_runner = ZipFileResolveRunner(RECURSIVE_ZIP, collection_dir, MOCK_CONFIG, MOCK_LOGGER)
        collection = zip_file_resolve_runner.run()

        good_nc = os.path.join(collection_dir, 'layer1', os.path.basename(GOOD_NC))
        bad_nc = os.path.join(collection_dir, 'layer1/layer2', os.path.basename(BAD_NC))

        self.assertEqual(len(collection), 2)

        self.assertEqual(collection[0].src_path, good_nc)
        self.assertTrue(os.path.exists(good_nc))

        self.assertEqual(collection[1].src_path, bad_nc)
        self.assertTrue(os.path.exists(bad_nc))
