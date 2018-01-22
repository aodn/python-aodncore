import os
import re
from collections import OrderedDict
from tempfile import mkstemp

from .basestep import AbstractCollectionStepRunner
from ..exceptions import InvalidHarvesterError, UnmappedFilesError
from ..files import PipelineFileCollection, validate_pipelinefilecollection
from ...util import mkdir_p, LoggingContext, SystemProcess, TemporaryDirectory

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

        self._logger.sysinfo("harvesting slice size: {slice_size}".format(slice_size=self.slice_size))
        deletion_slices = deletions.get_slices(self.slice_size)
        addition_slices = additions.get_slices(self.slice_size)

        for file_slice in deletion_slices:
            deletion_map = self.match_harvester_to_files(file_slice)
            self.validate_harvester_mapping(file_slice, deletion_map)
            self.run_deletions(deletion_map, self.tmp_base_dir)

        for file_slice in addition_slices:
            addition_map = self.match_harvester_to_files(file_slice)
            self.validate_harvester_mapping(file_slice, addition_map)
            self.run_additions(addition_map, self.tmp_base_dir)

    def match_harvester_to_files(self, file_list):
        harvester_map = OrderedDict()

        for harvester, config_item in self._config.trigger_config.items():
            matched_files = PipelineFileCollection()

            for event in config_item['events']:
                for config_type, patterns in event.items():
                    if config_type == 'regex':
                        for pattern in patterns:
                            matched_files_for_pattern = file_list.filter_by_attribute_regex('dest_path', pattern)
                            if matched_files_for_pattern:
                                for mf in matched_files_for_pattern:
                                    self._logger.sysinfo("harvester '{harvester}' matched file: {mf.src_path}".format(
                                        harvester=harvester, mf=mf))

                                matched_files.update(matched_files_for_pattern, overwrite=True)

                            # TODO: implement extra parameters

            if matched_files:
                harvester_map[harvester] = matched_files

        return harvester_map

    @staticmethod
    def validate_harvester_mapping(file_collection, harvester_map):
        all_matched_files = PipelineFileCollection()
        for matched_files in harvester_map.values():
            all_matched_files.update(matched_files, overwrite=True)

        if not file_collection.issubset(all_matched_files):
            unmapped_files = [m.src_path for m in file_collection - all_matched_files]
            raise UnmappedFilesError(
                "no matching harvester(s) found for: {unmapped_files}".format(unmapped_files=unmapped_files))

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

    @staticmethod
    def create_input_file_list(talend_base_dir, matched_file_list):
        _, input_file_list = mkstemp(prefix='file_list', suffix='.txt', dir=talend_base_dir)

        with open(input_file_list, 'w') as f:
            f.writelines("{line}{sep}".format(line=l, sep=os.linesep) for l in matched_file_list)

        return input_file_list

    def undo_processed_files(self):
        for file_map in self.harvested_file_map:
            for file_collection in file_map.values():
                file_collection.set_bool_attribute('should_undo', True)

            self.run_undo_deletions(file_map)

    def execute_talend(self, executor, matched_files, talend_base_dir, success_attribute='is_harvested'):
        matched_file_list = [mf.dest_path for mf in matched_files]
        input_file_list = self.create_input_file_list(talend_base_dir, matched_file_list)

        converted_exec = self.executor_conversion(executor)

        talend_exec = converted_exec.format(base=talend_base_dir, file_list=input_file_list,
                                            log_dir=self._config.pipeline_config['talend']['talend_log_dir'])

        self._logger.sysinfo("executing {talend_exec}".format(talend_exec=talend_exec))

        p = SystemProcess(talend_exec, shell=True)

        self._logger.info('--- START TALEND OUTPUT ---')
        with LoggingContext(self._logger, format_='%(message)s'):
            try:
                p.execute()
            except Exception:
                self._logger.error(p.stdout_text)
                raise
            else:
                matched_files.set_bool_attribute(success_attribute, True)
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
                self.execute_talend(self._config.trigger_config[harvester]['exec'], matched_files, talend_base_dir,
                                    'is_harvest_undone')

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
                except Exception:
                    self.undo_processed_files()
                    raise

            self.harvested_file_map.append({harvester: matched_files})

            files_to_upload = matched_files.filter_by_bool_attribute('pending_store_addition')
            if files_to_upload:
                self.upload_runner.run(files_to_upload)
