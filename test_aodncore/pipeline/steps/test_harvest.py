import os

from mock import MagicMock, patch

from aodncore.common import SystemCommandFailedError
from aodncore.pipeline import PipelineFile, PipelineFileCollection, PipelineFilePublishType
from aodncore.pipeline.exceptions import InvalidHarvesterError, InvalidHandlerError
from aodncore.pipeline.steps.harvest import get_harvester_runner, TalendHarvesterRunner
from aodncore.testlib import BaseTestCase, get_test_config, mock
from test_aodncore import TESTDATA_DIR

TEST_ROOT = os.path.dirname(__file__)
GOOD_NC = os.path.join(TESTDATA_DIR, 'good.nc')
PF_1_NC = os.path.join(TESTDATA_DIR, 'pf1.nc')
PF_2_NC = os.path.join(TESTDATA_DIR, 'pf2.nc')


def get_harvest_collection(delete=False):
    pipeline_file = PipelineFile(GOOD_NC, is_deletion=delete)
    publish_type = PipelineFilePublishType.UNHARVEST_ONLY if delete else PipelineFilePublishType.HARVEST_ONLY
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
        with self.assertRaises(InvalidHandlerError):
            harvester_runner.validate_file_handling(file_slice, matched_file_map)

    def test_get_harvest_runner(self):
        harvester_runner = get_harvester_runner('talend', self.uploader, None, TESTDATA_DIR, None, self.mock_logger)
        self.assertIsInstance(harvester_runner, TalendHarvesterRunner)

    def test_get_harvest_runner_invalid(self):
        with self.assertRaises(InvalidHarvesterError):
            _ = get_harvester_runner('nonexistent_harvester', self.uploader, None, TESTDATA_DIR, None, self.mock_logger)

    @patch.object(TalendHarvesterRunner, 'execute_talend')
    def test_talend_harvester_single_addition(self, mock_execute_talend):
        harvest_collection = get_harvest_collection()
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_execute_talend.assert_called_once()

    @patch.object(TalendHarvesterRunner, 'execute_talend')
    def test_talend_harvester_single_deletion(self, mock_execute_talend):
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_execute_talend.assert_called_once()

    @patch.object(TalendHarvesterRunner, 'run_deletions')
    def test_single_store_deletion(self, mock_run_deletions):
        harvest_collection = get_harvest_collection(delete=True)
        harvested_file_map = {'my_test_harvester': harvest_collection}
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_run_deletions.assert_called_once_with(harvested_file_map, TESTDATA_DIR)

    @patch.object(TalendHarvesterRunner, 'match_harvester_to_files')
    @patch.object(TalendHarvesterRunner, 'run_deletions')
    def test_talend_harvester_single_deletion_no_map(self, mock_run_deletions, mock_match_harvester_to_files):
        mock_match_harvester_to_files.return_value = {}
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        harvester_runner.run(harvest_collection)
        mock_run_deletions.assert_not_called()

    def test_talend_harvester_single_deletion_exec_fail(self):
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_fail.conf')
        harvest_collection = get_harvest_collection(delete=True)
        harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            harvester_runner.run(harvest_collection)

    @patch.object(TalendHarvesterRunner, 'execute_talend')
    def test_multi_harvester_success(self, mock_execute_talend):
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_multi.conf')
        harvest_collection = get_harvest_collection()
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        multi_harvester_runner.run(harvest_collection)
        # Expect exactly 2 talend calls, one for each harvester
        assert mock_execute_talend.call_count == 2

    @patch.object(TalendHarvesterRunner, 'run_deletions')
    def test_harvester_exception_cleanup(self, mock_run_deletions):
        harvest_collection = get_harvest_collection()
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        # Expect cleanup attempt NOT to have been called, as no files have been harvested yet
        mock_run_deletions.assert_not_called()

    @patch.object(TalendHarvesterRunner, 'run_undo_deletions')
    def test_multi_harvester_exception_cleanup(self, mock_run_undo_deletions):
        harvest_collection = get_harvest_collection(False)
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_single_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        # Expect cleanup attempt to have been called on the files already harvested
        mock_run_undo_deletions.assert_called_once()

    @patch.object(TalendHarvesterRunner, 'run_undo_deletions')
    def test_harvester_exception_cleanup_previous_success(self, mock_run_undo_deletions):
        harvest_collection = get_harvest_collection()
        os.environ['PIPELINE_TRIGGER_CONFIG_FILE'] = os.path.join(TEST_ROOT, 'trigger_multi_fail.conf')
        multi_harvester_runner = TalendHarvesterRunner(self.uploader, None, TESTDATA_DIR, self.config, self.mock_logger)
        with self.assertRaises(SystemCommandFailedError):
            multi_harvester_runner.run(harvest_collection)
        # Expect both sets of already harvested files to have been deleted
        mock_run_undo_deletions.call_count = 2
