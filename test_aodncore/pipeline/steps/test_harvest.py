from __future__ import absolute_import
import os

from mock import patch

from aodncore.common import SystemCommandFailedError
from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import InvalidHarvesterError, UnmappedFilesError
from aodncore.pipeline.steps.harvest import (get_harvester_runner, HarvesterMap, TalendHarvesterRunner, TriggerEvent,
                                             validate_harvester_mapping)
from aodncore.pipeline.steps.store import StoreRunner
from aodncore.testlib import BaseTestCase, NullStorageBroker
from test_aodncore import TESTDATA_DIR

TEST_ROOT = os.path.dirname(__file__)
BAD_NC = os.path.join(TESTDATA_DIR, 'bad.nc')
EMPTY_NC = os.path.join(TESTDATA_DIR, 'empty.nc')
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')

HARVEST_SUCCESS = 0
HARVEST_FAIL = 1


def get_harvest_collection(delete=False, with_store=False, already_stored=False):
    pf_bad = PipelineFile(BAD_NC, is_deletion=delete)
    pf_empty = PipelineFile(EMPTY_NC, is_deletion=delete)
    pf_good = PipelineFile(GOOD_NC, is_deletion=delete)

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
        super(TestPipelineStepsHarvest, self).setUp()

        self.uploader = StoreRunner(NullStorageBroker("/"), None, None)

    def test_get_harvester_runner(self):
        harvester_runner = get_harvester_runner('talend', self.uploader, None, TESTDATA_DIR, None, self.test_logger)
        self.assertIsInstance(harvester_runner, TalendHarvesterRunner)

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


class TestTalendHarvesterRunner(BaseTestCase):
    def setUp(self):
        super(TestTalendHarvesterRunner, self).setUp()
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
