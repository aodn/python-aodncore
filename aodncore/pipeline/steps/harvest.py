import os
import re
from collections import OrderedDict
from tempfile import mkstemp

from .basestep import AbstractCollectionStepRunner
from ..exceptions import InvalidHarvesterError, InvalidHandlerError
from ..files import PipelineFileCollection, validate_pipelinefilecollection
from ...common import SystemCommandFailedError
from ...util import mkdir_p, format_exception, LoggingContext, SystemProcess, TemporaryDirectory

__all__ = [
    'get_harvester_runner',
    'TalendHarvesterRunner'
]


def get_harvester_runner(harvester_name, upload_runner, harvest_params, tmp_base_dir, config, logger):
    """Factory function to return appropriate harvester class

    :param harvester_name: harvester name
    :param upload_runner: upload runner instance to use for uploads
    :param harvest_params: keyword parameters passed to harvest runner
    :param tmp_base_dir: base temporary directory
    :param config: LazyConfigManager instance
    :param logger: Logger instance
    :return: BaseHarvesterRunner sub-class
    """

    if harvester_name == 'talend':
        return TalendHarvesterRunner(upload_runner, harvest_params, tmp_base_dir, config, logger)
    else:
        raise InvalidHarvesterError("invalid harvester '{name}'".format(name=harvester_name))


# noinspection PyAbstractClass
class BaseHarvesterRunner(AbstractCollectionStepRunner):
    """Base class for HarvesterRunner classes
    """
    pass


class TalendHarvesterRunner(BaseHarvesterRunner):
    """HarvesterRunner implementation to execute Talend harvesters
    """

    def __init__(self, upload_runner, harvest_params, tmp_base_dir, config, logger, deletion=False):
        super(TalendHarvesterRunner, self).__init__(config, logger)
        if harvest_params is None:
            harvest_params = {}

        self.deletion = deletion
        self.slice_size = harvest_params.get('slice_size', 2048)
        self.params = harvest_params
        self.tmp_base_dir = tmp_base_dir
        self.upload_runner = upload_runner
        self.harvested_file_map = []

    def run(self, pipeline_files):
        """The entry point to the ported talend trigger code to execute the harvester(s) for each file
        
        :return: 
        """
        validate_pipelinefilecollection(pipeline_files)

        deletions = pipeline_files.filter_by_bool_attribute('pending_harvest_deletion')
        additions = pipeline_files.filter_by_bool_attribute('pending_harvest_addition')

        self._logger.sysinfo("Dividing files into slices of {slice_size} files".format(slice_size=self.slice_size))
        deletion_slices = deletions.get_slices(self.slice_size)
        addition_slices = additions.get_slices(self.slice_size)

        for file_slice in deletion_slices:
            deletion_map = self.match_harvester_to_files(file_slice)
            if not deletion_map:
                self._logger.info('No files to delete, proceeding to add matching files')
            else:
                self.run_deletions(deletion_map, self.tmp_base_dir)

        for file_slice in addition_slices:
            addition_map = self.match_harvester_to_files(file_slice)
            self.validate_file_handling(file_slice, addition_map)

            for matched_files in addition_map.values():
                for f in matched_files:
                    self._logger.info("Adding file with destination path: {}".format(f.dest_path))

            self.run_additions(addition_map, self.tmp_base_dir)

    def match_harvester_to_files(self, file_list):
        harvester_map = OrderedDict()

        for harvester, config_item in self._config.trigger_config.items():
            matched_files = PipelineFileCollection()
            extra_params = []

            for event in config_item['events']:
                for config_type, patterns in event.items():
                    if config_type == 'regex':
                        for pattern in patterns:
                            matched_files_for_pattern = file_list.filter_by_attribute_regex('dest_path', pattern)
                            matched_files.update(matched_files_for_pattern, overwrite=True)

                            # TODO: implement extra parameters
                            # if config_type == 'extra_params':
                            # do some magic here
            if matched_files:
                harvester_map[harvester] = matched_files

        return harvester_map

    def validate_file_handling(self, file_collection, harvester_map):
        all_matched_files = PipelineFileCollection()
        for matched_files in harvester_map.values():
            all_matched_files.update(matched_files, overwrite=True)

        if file_collection.issubset(all_matched_files):
            self._logger.info('All files in slice mapped correctly to a harvester')
        else:
            missing_files = file_collection - all_matched_files
            for mf in missing_files:
                self._logger.error("File {mf.src_path} has not been mapped to a harvester".format(mf=mf))

            raise InvalidHandlerError('Not all files in the file slice have been mapped to a harvester')

    @staticmethod
    def create_symlink(talend_base_dir, src_path, dest_path):
        symlink_target = os.path.join(talend_base_dir, dest_path)
        index_dir = os.path.dirname(symlink_target)
        mkdir_p(index_dir)
        os.symlink(src_path, symlink_target)

    @staticmethod
    def executor_conversion(executor):
        python_formatted_exec = re.sub('=%{', '={', executor)
        return python_formatted_exec

    def create_input_file_list(self, talend_base_dir, matched_file_list):
        _, input_file_list = mkstemp(prefix='file_list', suffix='.txt', dir=talend_base_dir)

        with open(input_file_list, 'w') as f:
            self._logger.sysinfo('Files to process: ')
            self._logger.sysinfo(matched_file_list)
            f.writelines("{line}{sep}".format(line=l, sep=os.linesep) for l in matched_file_list)

        return input_file_list

    def cleanup_on_error(self):
        for file_map in self.harvested_file_map:
            for file_collection in file_map.values():
                file_collection.set_bool_attribute('should_undo', True)

            self.run_undo_deletions(file_map)

    def execute_talend(self, executor, matched_files, talend_base_dir):
        matched_file_list = [mf.dest_path for mf in matched_files]
        input_file_list = self.create_input_file_list(talend_base_dir, matched_file_list)

        converted_exec = self.executor_conversion(executor)

        talend_exec = converted_exec.format(base=talend_base_dir, file_list=input_file_list,
                                            log_dir=self._config.pipeline_config['talend']['talend_log_dir'])

        self._logger.sysinfo("Executing {talend_exec}".format(talend_exec=talend_exec))

        p = SystemProcess(talend_exec, shell=True)

        self._logger.info('--- START TALEND OUTPUT ---')
        with LoggingContext(self._logger, format_='%(message)s'):
            try:
                p.execute()
            except Exception:
                self._logger.error(p.stdout_text)
                raise
            else:
                self._logger.info(p.stdout_text)
            finally:
                self._logger.info('--- END TALEND OUTPUT ---')

    def run_deletions(self, harvester_map, tmp_base_dir):
        """Function to un-harvest and delete files using the appropriate file upload runner
        Operates in newly created temporary directory as talend requires a non-existant file to perform un-harvesting


        :param harvester_map: mapping of harvesters to the PipelineFileCollection to operate on
        :param tmp_base_dir: temporary directory base for talend operation
        """
        for harvester, matched_files in harvester_map.items():
            with TemporaryDirectory(prefix='talend_base', dir=tmp_base_dir) as talend_base_dir:
                self.execute_talend(self._config.trigger_config[harvester]['exec'], matched_files, talend_base_dir)

            files_to_delete = matched_files.filter_by_bool_attribute('pending_store_deletion')
            if files_to_delete:
                self.upload_runner.run(files_to_delete)

    def run_undo_deletions(self, harvester_map):
        """Function to un-harvest and undo stored files as appropriate in the case of errors
        Operates in newly created temporary directory as talend requires a non-existant file to perform un-harvesting


        :param harvester_map: mapping of harvesters to the PipelineFileCollection to operate on
        """
        for harvester, matched_files in harvester_map.items():
            with TemporaryDirectory(prefix='talend_base', dir=self.tmp_base_dir) as talend_base_dir:
                self.execute_talend(self._config.trigger_config[harvester]['exec'], matched_files, talend_base_dir)

            files_to_delete = matched_files.filter_by_bool_attributes_and('pending_undo', 'is_stored')
            if files_to_delete:
                self.upload_runner.run(files_to_delete)

    def run_additions(self, harvester_map, tmp_base_dir):
        """Function to harvest and upload files using the appropriate file upload runner
        Operates in newly created temporary directory and creates symlink between source and destination file
        Talend will then operate on the destination file (symlink)


        :param harvester_map: mapping of harvesters to the PipelineFileCollection to operate on
        :param tmp_base_dir: temporary directory base for talend operation
        """
        for harvester, matched_files in harvester_map.items():
            with TemporaryDirectory(prefix='talend_base', dir=tmp_base_dir) as talend_base_dir:
                for pf in matched_files:
                    self.create_symlink(talend_base_dir, pf.src_path, pf.dest_path)

                try:
                    self.execute_talend(self._config.trigger_config[harvester]['exec'], matched_files, talend_base_dir)
                    matched_files.set_bool_attribute('is_harvested', True)
                except SystemCommandFailedError:
                    self.cleanup_on_error()
                    raise

            self.harvested_file_map.append({harvester: matched_files})

            files_to_upload = matched_files.filter_by_bool_attribute('pending_store_addition')
            if files_to_upload:
                self.upload_runner.run(files_to_upload)
