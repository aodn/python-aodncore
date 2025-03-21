import json
import os
from unittest.mock import patch
from aodncore.common import SystemCommandFailedError
from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import InvalidHarvesterError, UnmappedFilesError, InvalidConfigError, \
    MissingConfigFileError, MissingConfigParameterError, UnexpectedCsvFilesError, GeonetworkConnectionError, \
    InvalidSQLConnectionError
from aodncore.pipeline.steps.harvest import (get_harvester_runner, HarvesterMap, TalendHarvesterRunner, TriggerEvent,
                                             validate_harvester_mapping, CsvHarvesterRunner)
from aodncore.pipeline.steps.store import StoreRunner
from aodncore.testlib import BaseTestCase, NullStorageBroker
from aodncore.util import WriteOnceOrderedDict

from test_aodncore import TESTDATA_DIR

TEST_ROOT = os.path.dirname(__file__)
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
EMPTY_NC = os.path.join(TESTDATA_DIR, 'empty.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')

HARVEST_SUCCESS = 0
HARVEST_FAIL = 1


def get_harvest_collection(delete=False, late_deletion=False, with_store=False, already_stored=False):
    pf_bad = PipelineFile(BAD_NC, is_deletion=delete, late_deletion=late_deletion)
    pf_empty = PipelineFile(EMPTY_NC, is_deletion=delete, late_deletion=late_deletion)
    pf_good = PipelineFile(GOOD_NC, is_deletion=delete, late_deletion=late_deletion)

    collection = PipelineFileCollection([pf_bad, pf_empty, pf_good])

    if with_store:
        publish_type = PipelineFilePublishType.DELETE_UNHARVEST if delete else PipelineFilePublishType.HARVEST_UPLOAD
    else:
        publish_type = PipelineFilePublishType.UNHARVEST_ONLY if delete else PipelineFilePublishType.HARVEST_ONLY

    for pipeline_file in collection:
        pipeline_file.is_stored = already_stored
        pipeline_file.dest_path = os.path.join('DUMMY', os.path.basename(pipeline_file.src_path))
        pipeline_file.publish_type = publish_type

    return collection


class TestPipelineStepsHarvest(BaseTestCase):
    def setUp(self):
        super().setUp()

        self.uploader = StoreRunner(NullStorageBroker("/"), None, None)

    def test_get_harvester_runner(self):
        talend_runner = get_harvester_runner('talend', self.uploader, None, TESTDATA_DIR, None, self.test_logger)
        self.assertIsInstance(talend_runner, TalendHarvesterRunner)

        csv_runner = get_harvester_runner('csv', self.uploader, None, TESTDATA_DIR, None, self.test_logger)
        self.assertIsInstance(csv_runner, CsvHarvesterRunner)

    def test_get_harvester_runner_csv(self):
        harvester_runner = get_harvester_runner('csv', self.uploader, None, TESTDATA_DIR, None, self.test_logger)
        self.assertIsInstance(harvester_runner, CsvHarvesterRunner)

    def test_get_harvester_runner_invalid(self):
        with self.assertRaises(InvalidHarvesterError):
            _ = get_harvester_runner('nonexistent_harvester', self.uploader, None, TESTDATA_DIR, None, self.test_logger)

    def test_validate_harvester_mapping(self):
        collection = get_harvest_collection()
        subset = collection.filter_by_attribute_value('src_path', GOOD_NC)

        matched_file_map = HarvesterMap()
        matched_file_map.add_event('my_test_harvester', TriggerEvent(subset))

        with self.assertRaises(UnmappedFilesError):
            validate_harvester_mapping(collection, matched_file_map)


class TestCsvHarvesterRunner(BaseTestCase):
    def setUp(self):
        super().setUp()

        self.uploader = NullStorageBroker("/")

        # assumes that username and password are supplied externally in one of the ways supported by libpq, such as
        # a ~/.pgpass file or environment variables

        os.environ['PGHOST'] = 'PGHOST'
        os.environ['PGDATABASE'] = 'PGDATABASE'
        os.environ['PGSSLMODE'] = 'require'

        self.harvester = CsvHarvesterRunner(self.uploader, None, self.config, self.test_logger)

    def test_harvester(self):
        collection = PipelineFileCollection([
            PipelineFile(self.temp_nc_file, publish_type=PipelineFilePublishType.HARVEST_ONLY),
            PipelineFile(GOOD_NC, publish_type=PipelineFilePublishType.UNHARVEST_ONLY, is_deletion=True),
            PipelineFile(BAD_NC, publish_type=PipelineFilePublishType.UNHARVEST_ONLY, is_deletion=True, late_deletion=True)
        ])
        self.harvester.run(collection)

        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_stored for f in collection))


class TestTalendHarvesterRunner(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.uploader = NullStorageBroker("/")

    @patch('aodncore.util.process.subprocess')
    def test_extra_params(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)
        harvester_runner.run(collection)

        expected_extra_params = "--collection my_test_collection"
        self.assertEqual(expected_extra_params,
                         harvester_runner.harvested_file_map.map['aaa_my_test_harvester'][0].extra_params)

        called_commands = [c[1][0] for c in mock_subprocess.Popen.mock_calls if c[1]]

        self.assertTrue(called_commands[0].startswith('echo zzz_my_test_harvester '))
        self.assertFalse(called_commands[0].endswith(expected_extra_params))

        self.assertTrue(called_commands[1].startswith('echo aaa_my_test_harvester '))
        self.assertTrue(called_commands[1].endswith(expected_extra_params))

        self.assertTrue(called_commands[2].startswith('echo aaa_my_test_harvester '))
        self.assertFalse(called_commands[2].endswith(expected_extra_params))

        self.assertTrue(called_commands[3].startswith('echo mmm_my_test_harvester '))
        self.assertFalse(called_commands[3].endswith(expected_extra_params))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_deletion(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertTrue(all(f.is_deletion for f in collection))
        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_stored for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_deletion_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertTrue(all(f.is_deletion for f in collection))
        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_stored for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_deletion(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(delete=True, with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(0)
        harvester_runner.storage_broker.assert_delete_call_count(1)

        self.assertTrue(all(f.is_deletion for f in collection))
        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_deleted for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_deletion_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(delete=True, with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(0)
        harvester_runner.storage_broker.assert_delete_call_count(3)

        self.assertTrue(all(f.is_deletion for f in collection))
        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_deleted for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_late_deletion(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True, delete=True)
        collection[2]._late_deletion = True

        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config, self.test_logger)
        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(0)
        harvester_runner.storage_broker.assert_delete_call_count(3)

        self.assertTrue(all(f.is_deletion for f in collection))
        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_deleted for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_late_deletion_not_run_with_addition_error(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (HARVEST_SUCCESS,  # slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS,  # slice 1, mmm_my_test_harvester, event 1
                                                    HARVEST_FAIL,  # failure slice 2, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS)  # undo slice 1, mmm_my_test_harvester, event 1
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        collection[0]._is_deletion = True
        collection[0]._late_deletion = False
        collection[2]._is_deletion = True
        collection[2]._late_deletion = True

        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config, self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(0)
        harvester_runner.storage_broker.assert_delete_call_count(1)

        # early deletion should have been triggered (i.e. run before additions)
        self.assertTrue(all((collection[0].is_deletion, collection[0].is_harvested, collection[0].is_stored)))
        # addition causes an error
        self.assertTrue(all((not collection[1].is_deletion, not collection[1].is_harvested, not collection[1].is_stored)))
        # late deletion should *not* have been triggered due to addition error
        self.assertTrue(all((collection[2].is_deletion, not collection[2].is_harvested, not collection[2].is_stored)))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_fail(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_FAIL
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertFalse(any(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_stored for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_fail_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_FAIL
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertFalse(any(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_uploaded for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_fail(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_FAIL
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertFalse(any(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_uploaded for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_fail_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_FAIL
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertFalse(any(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_uploaded for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_success(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_uploaded for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_success_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = 0
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertFalse(any(f.is_uploaded for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_success(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(1)
        harvester_runner.storage_broker.assert_delete_call_count(0)

        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_uploaded for f in collection))

        self.assertFalse(any(f.is_harvest_undone for f in collection))
        self.assertFalse(any(f.is_upload_undone for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_success_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.return_value = HARVEST_SUCCESS
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(3)
        harvester_runner.storage_broker.assert_delete_call_count(0)

        self.assertTrue(all(f.is_harvested for f in collection))
        self.assertTrue(all(f.is_uploaded for f in collection))

        self.assertFalse(any(f.is_harvest_undone for f in collection))
        self.assertFalse(any(f.is_upload_undone for f in collection))

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_undo(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (1, 0)
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertTrue(all(f.is_harvest_undone for f in collection))  # *should* be undone

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_undo_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (HARVEST_SUCCESS,  # slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS,  # slice 1, mmm_my_test_harvester, event 1
                                                    HARVEST_FAIL,  # failure slice 2, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # undo slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS)  # undo slice 1, mmm_my_test_harvester, event 1

        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        success_slice, fail_slice, pending_slice = collection.get_slices(harvester_runner.slice_size)

        self.assertTrue(all(f.is_harvested for f in success_slice))
        self.assertTrue(all(f.is_harvest_undone for f in success_slice))  # *should* be undone

        self.assertFalse(all(f.is_harvested for f in fail_slice))
        self.assertTrue(all(f.is_harvest_undone for f in fail_slice))  # *should* be undone

        self.assertFalse(all(f.is_harvested for f in pending_slice))
        self.assertFalse(all(f.is_harvest_undone for f in pending_slice))  # should *not* be undone, since never 'done'

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_undo(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (1, 0)
        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        self.assertFalse(all(f.is_harvested for f in collection))
        self.assertFalse(all(f.is_uploaded for f in collection))
        self.assertTrue(all(f.is_harvest_undone for f in collection))  # *should* be undone
        self.assertFalse(all(f.is_upload_undone for f in collection))  # should *not* be undone, since never 'done'

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_undo_sliced(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (HARVEST_SUCCESS,  # zzz_my_test_harvester, event 1, slice 1
                                                    HARVEST_SUCCESS,  # aaa_my_test_harvester, event 1, slice 1
                                                    HARVEST_SUCCESS,  # aaa_my_test_harvester, event 2, slice 1
                                                    HARVEST_SUCCESS,  # mmm_my_test_harvester, event 1, slice 1
                                                    HARVEST_FAIL,  # failure zzz_my_test_harvester, event 1, slice 2
                                                    HARVEST_SUCCESS,  # undo zzz_my_test_harvester, event 1, slice 2
                                                    HARVEST_SUCCESS,  # undo zzz_my_test_harvester, event 1, slice 1
                                                    HARVEST_SUCCESS,  # undo aaa_my_test_harvester, event 1, slice 1
                                                    HARVEST_SUCCESS,  # undo aaa_my_test_harvester, event 2, slice 1
                                                    HARVEST_SUCCESS)  # undo mmm_my_test_harvester, event 1, slice 1

        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1}, TESTDATA_DIR, self.config,
                                                 self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(1)
        harvester_runner.storage_broker.assert_delete_call_count(1)

        success_slice, fail_slice, pending_slice = collection.get_slices(harvester_runner.slice_size)

        self.assertTrue(all(f.is_harvested for f in success_slice))
        self.assertTrue(all(f.is_uploaded for f in success_slice))
        self.assertTrue(all(f.is_harvest_undone for f in success_slice))  # *should* be undone
        self.assertTrue(all(f.is_upload_undone for f in success_slice))  # *should* be undone

        self.assertFalse(all(f.is_harvested for f in fail_slice))
        self.assertFalse(all(f.is_uploaded for f in fail_slice))
        self.assertTrue(all(f.is_harvest_undone for f in fail_slice))  # *should* be undone
        self.assertFalse(all(f.is_upload_undone for f in fail_slice))  # should *not* be undone, since never 'done'

        self.assertFalse(all(f.is_harvested for f in pending_slice))
        self.assertFalse(all(f.is_uploaded for f in pending_slice))
        self.assertFalse(all(f.is_harvest_undone for f in pending_slice))  # should *not* be undone, since never 'done'
        self.assertFalse(all(f.is_upload_undone for f in pending_slice))  # should *not* be undone, since never 'done'

    @patch('aodncore.util.process.subprocess')
    def test_harvest_only_undo_only_current_slice(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (HARVEST_SUCCESS,  # slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS,  # slice 1, mmm_my_test_harvester, event 1
                                                    HARVEST_FAIL,  # failure slice 2, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS)  # undo slice 2, zzz_my_test_harvester, event 1

        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1, 'undo_previous_slices': False},
                                                 TESTDATA_DIR, self.config, self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_not_called()
        harvester_runner.storage_broker.assert_delete_not_called()

        success_slice, fail_slice, pending_slice = collection.get_slices(harvester_runner.slice_size)

        self.assertTrue(all(f.is_harvested for f in success_slice))
        self.assertFalse(any(f.is_harvest_undone for f in success_slice))  # should *not* be undone, due to param

        self.assertFalse(all(f.is_harvested for f in fail_slice))
        self.assertTrue(all(f.is_harvest_undone for f in fail_slice))  # *should* be undone

        self.assertFalse(all(f.is_harvested for f in pending_slice))
        self.assertFalse(all(f.is_harvest_undone for f in pending_slice))  # should *not* be undone, since never 'done'

    @patch('aodncore.util.process.subprocess')
    def test_harvest_upload_undo_only_current_slice(self, mock_subprocess):
        mock_subprocess.Popen().wait.side_effect = (HARVEST_SUCCESS,  # slice 1, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 1
                                                    HARVEST_SUCCESS,  # slice 1, aaa_my_test_harvester, event 2
                                                    HARVEST_SUCCESS,  # slice 1, mmm_my_test_harvester, event 1
                                                    HARVEST_FAIL,  # failure slice 2, zzz_my_test_harvester, event 1
                                                    HARVEST_SUCCESS)  # undo slice 2, zzz_my_test_harvester, event 1

        mock_subprocess.Popen().communicate.return_value = ('mocked stdout', 'mocked stderr')

        collection = get_harvest_collection(with_store=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, {'slice_size': 1, 'undo_previous_slices': False},
                                                 TESTDATA_DIR, self.config, self.test_logger)

        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(collection)

        harvester_runner.storage_broker.assert_upload_call_count(1)
        # no delete call expected, since fail slice failed during harvesting, and did not call upload
        harvester_runner.storage_broker.assert_delete_call_count(0)

        success_slice, fail_slice, pending_slice = collection.get_slices(harvester_runner.slice_size)

        self.assertTrue(all(f.is_harvested for f in success_slice))
        self.assertTrue(all(f.is_uploaded for f in success_slice))
        self.assertFalse(any(f.is_harvest_undone for f in success_slice))  # should *not* be undone, due to param
        self.assertFalse(any(f.is_upload_undone for f in success_slice))  # should *not* be undone, due to param

        self.assertFalse(all(f.is_harvested for f in fail_slice))
        self.assertFalse(any(f.is_uploaded for f in fail_slice))
        self.assertTrue(all(f.is_harvest_undone for f in fail_slice))  # *should* be undone
        self.assertFalse(any(f.is_upload_undone for f in fail_slice))  # should *not* be undone, since never 'done'

        self.assertFalse(any(f.is_harvested for f in pending_slice))
        self.assertFalse(any(f.is_uploaded for f in pending_slice))
        self.assertFalse(any(f.is_harvest_undone for f in pending_slice))  # should *not* be undone, since never 'done'
        self.assertFalse(any(f.is_upload_undone for f in pending_slice))  # should *not* be undone, since never 'done'


GOOD_CSV = os.path.join(TESTDATA_DIR, 'conn', 'test_table.csv')
ANOTHER_CSV = os.path.join(TESTDATA_DIR, 'conn', 'another_table.csv')


def get_csv_harvest_collection(with_store=False, already_stored=False, additional_files=None):
    pfc = [PipelineFile(GOOD_CSV)]
    if additional_files and isinstance(additional_files, list):
        for f in additional_files:
            pfc.append(PipelineFile(f))

    collection = PipelineFileCollection(pfc)

    if with_store:
        publish_type = PipelineFilePublishType.HARVEST_UPLOAD
    else:
        publish_type = PipelineFilePublishType.HARVEST_ONLY

    for pipeline_file in collection:
        pipeline_file.is_stored = already_stored
        pipeline_file.dest_path = os.path.join('DUMMY', os.path.basename(pipeline_file.src_path))
        pipeline_file.publish_type = publish_type

    return collection


GOOD_HARVEST_PARAMS = os.path.join(TESTDATA_DIR, 'test.harvest_params')
BAD_HARVEST_PARAMS = os.path.join(TESTDATA_DIR, 'invalid.harvest_params.nodbobjects')
INCOMPLETE_HARVEST_PARAMS = os.path.join(TESTDATA_DIR, 'test.harvest_params_incomplete')
RECURSIVE_HARVEST_PARAMS = os.path.join(TESTDATA_DIR, 'test.recursive_harvest_params')

class dummy_config(object):
    def __init__(self):
        self.pipeline_config = {
                'harvester': {
                    "config_dir": TESTDATA_DIR,
                    "schema_base_dir": TESTDATA_DIR
                }
            }


class TestCsvHarvesterRunner(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.uploader = NullStorageBroker("/")

    def compare_properties(self, left, right, name):
        return self.assertEqual(left.get(name), right.get(name, 'none_is_not_none'))

    def test_harvest_runner_params(self):
        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
            harvester_runner = CsvHarvesterRunner(self.uploader, hp, self.config, self.test_logger)

        self.assertIsNotNone(harvester_runner.params)
        self.assertIsNotNone(harvester_runner.storage_broker)
        self.assertIsNotNone(harvester_runner.config)
        self.assertIsNotNone(harvester_runner.logger)
        self.assertIsNotNone(harvester_runner.db_objects)

        # harvest_params specific
        for attr in ['db_schema', 'ingest_type', 'db_objects']:
            self.compare_properties(hp, harvester_runner.params, attr)

    def test_get_db_config(self):
        harvester_runner = CsvHarvesterRunner(self.uploader, {'db_schema': 'conn'}, dummy_config(),
                                              self.test_logger)
        with self.assertNoException():
            self.assertIsNotNone(harvester_runner.get_config_file('database.json'))

    def test_get_db_config_invalid(self):
        harvester_runner = CsvHarvesterRunner(self.uploader, {'db_schema': 'not_a_real_schema'}, dummy_config(),
                                              self.test_logger)
        with self.assertRaises(MissingConfigFileError):
            harvester_runner.get_config_file('database.json')

    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_get_process_sequence(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True
        replace = ["drop_object", "create_table_from_yaml_file", "load_data_from_csv", "execute_sql_file"]
        harvester_runner = CsvHarvesterRunner(self.uploader, {'ingest_type': 'replace'}, self.config, self.test_logger)

        self.assertEqual(replace, harvester_runner.get_process_sequence(mock_db))

    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_get_process_sequence_invalid(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True
        harvester_runner = CsvHarvesterRunner(
            self.uploader, {'ingest_type': 'bad_value'}, self.config, self.test_logger)

        with self.assertRaises(InvalidConfigError):
            harvester_runner.get_process_sequence(mock_db)

    def test_recursive_dependencies(self):
        with open(RECURSIVE_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
            harvester_runner = CsvHarvesterRunner(self.uploader, hp, self.config, self.test_logger)

        child = next(filter(lambda x: x['name'] == 'child', harvester_runner.db_objects))
        grandchild = next(filter(lambda x: x['name'] == 'grandchild', harvester_runner.db_objects))
        greatgrandchild = next(filter(lambda x: x['name'] == 'greatgrandchild', harvester_runner.db_objects))
        secondcousin = next(filter(lambda x: x['name'] == 'secondcousin', harvester_runner.db_objects))
        # child and grandchild should list test_table as a dependency, but secondcousing should not
        self.assertTrue('test_table' in child.get('dependencies'))
        self.assertTrue('test_table' in grandchild.get('dependencies'))
        self.assertTrue('test_table' in greatgrandchild.get('dependencies'))
        self.assertFalse('test_table' in secondcousin.get('dependencies'))
        # secondcousin and greatgrandchild should also have cousin as a dependency
        self.assertIn('cousin', secondcousin.get('dependencies'))
        self.assertIn('cousin', greatgrandchild.get('dependencies'))

    def test_build_runsheet(self):
        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
            harvester_runner = CsvHarvesterRunner(self.uploader, hp, self.config, self.test_logger)

        collection = get_csv_harvest_collection()
        for c in collection:
            harvester_runner.build_runsheet(c)

        # Runsheet should only include test_table and test_view
        included_objects = [o.get('name')
                            for o in harvester_runner.db_objects
                            if o.get('include')]
        self.assertEqual(included_objects, ['test_table', 'test_view'])

    def test_build_runsheet_recursive(self):
        with open(RECURSIVE_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
            harvester_runner = CsvHarvesterRunner(self.uploader, hp, self.config, self.test_logger)

        collection = get_csv_harvest_collection()
        for c in collection:
            harvester_runner.build_runsheet(c)

        # Runsheet should only include test_table and its dependents
        included_objects = [o.get('name')
                            for o in harvester_runner.db_objects
                            if o.get('include')]
        self.assertEqual(included_objects, ['test_table', 'child', 'grandchild', 'greatgrandchild'])

    @patch('aodncore.pipeline.steps.harvest.GeonetworkMetadataHandler')
    @patch('aodncore.pipeline.steps.harvest.Geonetwork')
    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_run_harvester(self, mock_db, mock_gn, mock_mh):
        mock_db.return_value.compare_schemas.return_value = True

        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
        collection = get_csv_harvest_collection()
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)

        with self.assertNoException():
            harvester_runner.run(collection)

        self.assertTrue(mock_db.called)
        self.assertTrue(mock_gn.called)
        self.assertTrue(mock_mh.called)

    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_run_harvester_no_db_objects(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True

        with open(BAD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
        collection = get_csv_harvest_collection()
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)

        with self.assertRaises(MissingConfigParameterError):
            harvester_runner.run(collection)

    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_run_harvester_unexpected_pipeline_files(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True

        with open(INCOMPLETE_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
        collection = get_csv_harvest_collection(additional_files=[ANOTHER_CSV])
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)

        with self.assertRaises(UnexpectedCsvFilesError):
            harvester_runner.run(collection)

    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_run_harvester_expected_pipeline_files(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True

        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
        collection = get_csv_harvest_collection(additional_files=[ANOTHER_CSV])
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)

        with self.assertNoException():
            harvester_runner.run(collection)


    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_harvest_upload(self, mock_db):
        mock_db.return_value.compare_schemas.return_value = True

        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f)
            hp.pop("metadata_updates")
        collection = get_csv_harvest_collection(with_store=True)
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)
        harvester_runner.run(collection)
        harvester_runner.storage_broker.assert_upload_call_count(1)

    @patch('aodncore.pipeline.steps.harvest.GeonetworkMetadataHandler', side_effect=GeonetworkConnectionError())
    @patch('aodncore.pipeline.steps.harvest.Geonetwork')
    @patch('aodncore.pipeline.steps.harvest.DatabaseInteractions')
    def test_geonetwork_catch_exception(self, mock_db, mock_gn, mock_mh):
        mock_db.return_value.compare_schemas.return_value = True

        with open(GOOD_HARVEST_PARAMS) as f:
            hp = json.load(f, object_pairs_hook=WriteOnceOrderedDict)
        collection = get_csv_harvest_collection(with_store=True)
        harvester_runner = CsvHarvesterRunner(self.uploader, hp, dummy_config(), self.test_logger)

        with self.assertNoException():
            harvester_runner.run(collection)

        self.assertTrue(mock_db.called)
        self.assertTrue(mock_gn.called)
        self.assertTrue(mock_mh.called)
        harvester_runner.storage_broker.assert_upload_call_count(1)
