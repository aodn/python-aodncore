import os

from mock import MagicMock, patch, mock_open

from aodncore.common import SystemCommandFailedError
from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import InvalidHarvesterError, UnmappedFilesError
from aodncore.pipeline.steps.harvest import get_harvester_runner, TalendHarvesterRunner
from aodncore.testlib import BaseTestCase, get_test_config
from test_aodncore import TESTDATA_DIR

TEST_ROOT = os.path.dirname(__file__)
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
PF_1_NC = os.path.join(TESTDATA_DIR, 'pf1.nc')
PF_2_NC = os.path.join(TESTDATA_DIR, 'pf2.nc')


def get_harvest_collection(delete=False):
    pipeline_file = PipelineFile(GOOD_NC, is_deletion=delete)
    publish_type = PipelineFilePublishType.DELETE_UNHARVEST if delete else PipelineFilePublishType.HARVEST_UPLOAD
    pipeline_file.publish_type = publish_type
    pipeline_file.dest_path = 'subdir/targetfile.nc'
    collection = PipelineFileCollection([pipeline_file])
    return collection


def get_storage_collection(undo=False):
    pipeline_file = PipelineFile(GOOD_NC)
    publish_type = PipelineFilePublishType.HARVEST_UPLOAD
    pipeline_file.should_undo = undo
    pipeline_file.is_stored = True
    pipeline_file.publish_type = publish_type
    pipeline_file.dest_path = 'subdir/targetfile.nc'
    collection = PipelineFileCollection([pipeline_file])
    return collection


def get_multi_file_slice():
    pf_1 = PipelineFile(PF_1_NC)
    pf_2 = PipelineFile(PF_2_NC)
    pf_1.publish_type = PipelineFilePublishType.HARVEST_ONLY
    pf_2.publish_type = PipelineFilePublishType.HARVEST_ONLY
    pf_1.dest_path = 'subdir/tf1.nc'
    pf_2.dest_path = 'subdir/tf2.nc'
    collection = PipelineFileCollection([pf_1, pf_2])
    return collection


class TestPipelineStepsHarvest(BaseTestCase):
    def setUp(self):
        super(TestPipelineStepsHarvest, self).setUp()
        self.uploader = MagicMock()
        # overwrite default config to allow loading of custom trigger.conf files
        self._config = get_test_config(self.temp_dir)

    def test_validate_file_handling_failure(self):
        matched_file_map = {'my_test_harvester': get_harvest_collection()}
        file_slice = get_multi_file_slice()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(UnmappedFilesError):
            harvester_runner.validate_harvester_mapping(file_slice, matched_file_map)

    def test_get_harvest_runner(self):
        harvester_runner = get_harvester_runner('talend', self.uploader, None, TESTDATA_DIR, None, self.mock_logger)
        self.assertIsInstance(harvester_runner, TalendHarvesterRunner)

    def test_get_harvest_runner_invalid(self):
        with self.assertRaises(InvalidHarvesterError):
            _ = get_harvester_runner('nonexistent_harvester', self.uploader, None, TESTDATA_DIR, None, self.mock_logger)

    @patch('aodncore.pipeline.steps.harvest.SystemProcess')
    def test_talend_harvester_single_addition(self, mock_systemprocess):
        harvest_collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_systemprocess.assert_called_once()
        self.assertTrue(all(f.is_harvested for f in harvest_collection))

    @patch('aodncore.pipeline.steps.harvest.SystemProcess')
    def test_talend_harvester_single_deletion(self, mock_systemprocess):
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_systemprocess.assert_called_once()
        self.assertTrue(all(f.is_harvested for f in harvest_collection))

    @patch('aodncore.pipeline.steps.harvest.SystemProcess')
    @patch('aodncore.pipeline.steps.harvest.TemporaryDirectory')
    @patch('aodncore.pipeline.steps.harvest.mkstemp')
    def test_single_store_deletion(self, mock_mkstemp, mock_temporarydirectory, mock_systemprocess):
        mock_mkstemp.return_value = ('', '')
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with patch('aodncore.pipeline.steps.harvest.open', mock_open(read_data='')) as m:
            harvester_runner.run(harvest_collection)

        self.assertTrue(all(f.is_harvested for f in harvest_collection))
        harvester_runner.upload_runner.run.assert_called_once_with(harvest_collection)

    @patch.object(TalendHarvesterRunner, 'match_harvester_to_files')
    @patch.object(TalendHarvesterRunner, 'run_deletions')
    def test_talend_harvester_single_deletion_no_map(self, mock_run_deletions, mock_match_harvester_to_files):
        mock_match_harvester_to_files.return_value = {}
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(UnmappedFilesError):
            harvester_runner.run(harvest_collection)
        mock_run_deletions.assert_not_called()

    def test_talend_harvester_single_deletion_exec_fail(self):
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_fail.conf')
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(harvest_collection)

    @patch('aodncore.pipeline.steps.harvest.SystemProcess')
    def test_multi_harvester_success(self, mock_systemprocess):
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_multi.conf')
        harvest_collection = get_harvest_collection()
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        multi_harvester_runner.run(harvest_collection)
        self.assertEqual(mock_systemprocess.call_count, 2)
        self.assertTrue(all(f.is_harvested for f in harvest_collection))

    @patch.object(TalendHarvesterRunner, 'run_deletions')
    def test_harvester_exception_cleanup(self, mock_run_deletions):
        harvest_collection = get_harvest_collection()
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        mock_run_deletions.assert_not_called()

    @patch.object(TalendHarvesterRunner, 'run_undo_deletions')
    def test_multi_harvester_exception_cleanup(self, mock_run_undo_deletions):
        harvest_collection = get_harvest_collection(False)
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_single_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        mock_run_undo_deletions.assert_called_once()

    def test_multi_harvest_undo(self):
        harvest_collection = get_harvest_collection(False)
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_single_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        self.assertTrue(harvest_collection[0].is_harvest_undone)

    def test_multi_storage_undo(self):
        harvest_collection = get_storage_collection(True)
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_single_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        self.assertTrue(harvest_collection[0].is_storage_undone)

    @patch.object(TalendHarvesterRunner, 'run_undo_deletions')
    def test_harvester_exception_cleanup_previous_success(self, mock_run_undo_deletions):
        harvest_collection = get_harvest_collection()
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_multi_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        self.assertEqual(mock_run_undo_deletions.call_count, 2)
