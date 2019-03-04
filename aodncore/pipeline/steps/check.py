"""This module provides the step runner classes for the :ref:`check` step.

Checking is performed by a :py:class:`BaseCheckRunner` class, which is used to determine whether a file conforms to a
"check", the definition of which is contained in the specific class. A check typically involves reading a file, and
testing whether the file conforms to some arbitrary criteria.

The most common use of this step is to test for compliance using the IOOS Compliance Checker.
"""

import abc
import itertools
import os
from functools import partial

from billiard.pool import Pool
from compliance_checker.runner import ComplianceChecker, CheckSuite

from .basestep import BaseStepRunner
from ..common import CheckResult, PipelineFileCheckType, validate_checktype
from ..exceptions import ComplianceCheckFailedError, InvalidCheckSuiteError, InvalidCheckTypeError
from ..files import PipelineFileCollection
from ...util import format_exception, is_netcdffile, is_nonemptyfile, CaptureStdIO

__all__ = [
    'get_async_compliance_checker_broker',
    'get_check_runner',
    'get_child_check_runner',
    'run_compliance_checks',
    'CheckRunnerAdapter',
    'ComplianceCheckerCheckRunner',
    'FormatCheckRunner',
    'MultiprocessingAsyncComplianceCheckerBroker',
    'NonEmptyCheckRunner'
]

CheckSuite.load_all_available_checkers()


def get_check_runner(config, logger, check_params=None):
    return CheckRunnerAdapter(config, logger, check_params)


def get_child_check_runner(check_type, config, logger, check_params=None):
    """Factory function to return appropriate checker class based on check type value

    :param check_type: :py:class:`PipelineFileCheckType` enum member
    :param check_params: dict of parameters to pass to :py:class:`BaseCheckRunner` class for runtime configuration
    :param config: :py:class:`LazyConfigManager` instance
    :param logger: :py:class:`Logger` instance
    :return: :py:class:`BaseCheckRunner` sub-class
    """
    validate_checktype(check_type)

    if check_type is PipelineFileCheckType.NC_COMPLIANCE_CHECK:
        return ComplianceCheckerCheckRunner(config, logger, check_params)
    elif check_type is PipelineFileCheckType.FORMAT_CHECK:
        return FormatCheckRunner(config, logger)
    elif check_type is PipelineFileCheckType.NONEMPTY_CHECK:
        return NonEmptyCheckRunner(config, logger)
    else:
        raise InvalidCheckTypeError("invalid check type '{check_type}'".format(check_type=check_type))


def get_async_compliance_checker_broker(async_mode, config, logger, check_params=None):
    """Factory function to return appropriate asynchronous Compliance Checker runner class based on async_mode value
    """
    if async_mode == 'pool':
        return MultiprocessingAsyncComplianceCheckerBroker(config, logger, check_params, pool_class=Pool)
    elif async_mode == 'celery':
        # TODO: implement celery backend based on as yet undeveloped 'checkservice'
        raise NotImplementedError
    else:
        raise ValueError("invalid async_mode '{async_mode}'".format(async_mode=async_mode))


class BaseCheckRunner(BaseStepRunner):
    """A CheckRunner is responsible for performing checks on a given collection of files.
    
    The 'run' method is supplied with a PipelineFileCollection object and performs arbitrary checks against the files, 
    with the only expectation being that it must update the PipelineFile elements' check_result property with a
    CheckResult instance.
    
    The 'compliant' attribute of the CheckResult instance is a simple boolean determining whether the file
    is compliant with the given arbitrary check, and 'compliance_log' must be a collection (e.g. list, tuple) containing
    arbitrary information about why the file is considered non-compliant. Note: 'compliance_log' is a collection type in
    order to correlate it to 'lines in a log file', and typically should return an empty tuple if the file is compliant.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def run(self, pipeline_files):
        pass


class CheckRunnerAdapter(BaseCheckRunner):
    def __init__(self, config, logger, check_params=None):
        super(CheckRunnerAdapter, self).__init__(config, logger)

        if check_params is None:
            check_params = {}

        self.check_params = check_params

    def run(self, pipeline_files):
        check_types = {t.check_type for t in pipeline_files if
                       t.check_type in PipelineFileCheckType.all_checkable_types}

        for check_type in check_types:
            check_list = pipeline_files.filter_by_attribute_id('check_type', check_type)
            check_runner = get_child_check_runner(check_type, self._config, self._logger, self.check_params)
            self._logger.sysinfo("get_child_check_runner -> {check_runner}".format(check_runner=check_runner))
            check_runner.run(check_list)

        failed_files = PipelineFileCollection(f for f in pipeline_files
                                              if f.check_type in check_types and not f.check_result.compliant)

        for f in failed_files:
            self._logger.error(u"log for failed file '{name}'{sep}{log}".format(name=f.name, sep=os.linesep,
                                                                                log=os.linesep.join(
                                                                                    f.check_result.log)))
        if failed_files:
            failed_list = failed_files.get_attribute_list('name')
            raise ComplianceCheckFailedError(
                "the following files failed the check step: {failed_list}".format(failed_list=failed_list))


def run_compliance_checks(file_path, checks, verbosity=0, criteria='normal', skip_checks=None, output_format='text'):
    """Run the given check suites on the given file, and consolidate the results into a single result

    Note: this function deliberately avoids references to PipelineFile objects, so that it can be more easily used
        by the asynchronous processing backends

    :return: :py:class:`aodncore.pipeline.CheckResult` object
    """
    if not checks:
        raise InvalidCheckSuiteError('compliance check requested but no check suite(s) specified')

    # first check that it is a valid NetCDF format file
    if not is_netcdffile(file_path):
        compliance_log = ("invalid NetCDF file",)
        overall_result = CheckResult(file_path, False, compliance_log)
        return overall_result

    check_results = []
    for check in checks:
        stdout_log = []
        stderr_log = []
        try:
            with CaptureStdIO() as (stdout_log, stderr_log):
                compliant, errors = ComplianceChecker.run_checker(file_path, [check],
                                                                  verbosity, criteria, skip_checks,
                                                                  output_format=output_format)
        except Exception as e:  # pragma: no cover
            errors = True
            stderr_log.extend([
                'WARNING: compliance checks did not complete due to error. {e}'.format(e=format_exception(e))
            ])

        # if any exceptions during checking, assume file is non-compliant
        if errors:
            compliant = False

        compliance_log = []
        if not compliant:
            compliance_log.extend(stdout_log)
            compliance_log.extend(stderr_log)

        check_results.append(CheckResult(file_path, compliant, compliance_log, errors))

    # check results
    compliant = all(r.compliant for r in check_results)
    compliance_log = list(itertools.chain.from_iterable(r.log for r in check_results))
    errors = any(r.errors for r in check_results)

    overall_result = CheckResult(file_path, compliant, compliance_log, errors)
    return overall_result


class BaseAsyncComplianceCheckerBroker(object):
    """This base class contains the common logic for submitting compliance checking tasks to asynchronous backends.
    """
    __metaclass__ = abc.ABCMeta

    def __init__(self, config, logger, check_params=None):
        if check_params is None:
            check_params = {}

        self.config = config
        self.logger = logger
        self._check_params = check_params

        self.checks = check_params.get('checks')
        self.verbosity = check_params.get('verbosity', 0)
        self.criteria = check_params.get('criteria', 'normal')
        self.skip_checks = check_params.get('skip_checks')
        self.output_format = check_params.get('output_format', 'text')

        self._check_function = partial(run_compliance_checks,
                                       checks=self.checks,
                                       verbosity=self.verbosity,
                                       criteria=self.criteria,
                                       skip_checks=self.skip_checks,
                                       output_format=self.output_format)

    def __repr__(self):
        return "{self.__class__.__name__}(check_params={self._check_params})".format(self=self)

    def _checker_callback(self, check_result):
        self.logger.info("completed compliance check of '{check_result.path}', "
                         "compliant: {check_result.compliant}".format(check_result=check_result))

    @abc.abstractmethod
    def _run(self, pipeline_files):
        """The child classes must override _run, and are expected to return an ordered list of CheckResult instances,
        corresponding to the input PipelineFileCollection

        :param pipeline_files: :py:class:`aodncore.pipeline.PipelineFileCollection`
        :return: None
        """
        pass

    def run(self, pipeline_files):
        check_results = self._run(pipeline_files)
        for check_result, pipeline_file in zip(check_results, pipeline_files):
            pipeline_file.check_result = check_result


class MultiprocessingAsyncComplianceCheckerBroker(BaseAsyncComplianceCheckerBroker):
    """Broker for delegating executions of the check function to the standard *Pool classes of the multiprocessing
    module (typically Pool or ThreadPool)
    """

    def __init__(self, config, logger, check_params, pool_class=Pool):
        super(MultiprocessingAsyncComplianceCheckerBroker, self).__init__(config, logger, check_params)
        self.pool_class = pool_class
        self.pool_process_count = config.pipeline_config.get('check', {}).get('pool_process_count', 1)

    def __repr__(self):
        return ("{self.__class__.__name__}(check_params={self._check_params}, "
                "pool_class={self.pool_class.__name__})").format(self=self)

    def _run(self, pipeline_files):
        pool = self.pool_class(self.pool_process_count)
        results = [pool.apply_async(self._check_function, (pf.src_path,), callback=self._checker_callback)
                   for pf in pipeline_files]
        check_results = [result.get() for result in results]
        return check_results


# TODO: implement celery backend for asynchronous compliance checking
class CeleryAsyncComplianceCheckerBroker(BaseAsyncComplianceCheckerBroker):
    def _run(self, pipeline_files):
        raise NotImplementedError


class ComplianceCheckerCheckRunner(BaseCheckRunner):
    def __init__(self, config, logger, check_params=None):
        super(ComplianceCheckerCheckRunner, self).__init__(config, logger)

        self.check_params = check_params or {}
        self.async_mode = config.pipeline_config.get('check', {}).get('async_mode', 'pool')

        self._validate_checks()

    def __repr__(self):
        return "{self.__class__.__name__}(check_params={self.check_params})".format(self=self)

    def _validate_checks(self):
        checks = self.check_params.get('checks')

        if not checks:
            raise InvalidCheckSuiteError('compliance check requested but no check suite(s) specified')

        # workaround a possible bug in the compliance checker where invalid check suites are ignored
        available_checkers = set(CheckSuite.checkers)
        these_checkers = set(checks)
        if not these_checkers.issubset(available_checkers):
            invalid_suites = list(these_checkers.difference(available_checkers))
            raise InvalidCheckSuiteError(
                'invalid compliance check suites: {invalid_suites}'.format(invalid_suites=invalid_suites))

    def run(self, pipeline_files):
        if self.check_params.get('skip_checks'):
            self._logger.info("compliance checks will skip {skip_checks}".format(**self.check_params))

        checker_broker = get_async_compliance_checker_broker(self.async_mode,
                                                             self._config,
                                                             self._logger,
                                                             self.check_params)
        self._logger.sysinfo(
            "get_async_compliance_checker_broker -> {checker_broker}".format(checker_broker=checker_broker))
        checker_broker.run(pipeline_files)


class FormatCheckRunner(BaseCheckRunner):
    def run(self, pipeline_files):
        for pipeline_file in pipeline_files:
            self._logger.info(
                "checking '{pipeline_file.src_path}' is a valid '{pipeline_file.file_type.name}' file".format(
                    pipeline_file=pipeline_file))
            compliant = pipeline_file.file_type.validator(pipeline_file.src_path)
            compliance_log = () if compliant else (
                "invalid format: did not validate as type: {pipeline_file.file_type.name}".format(
                    pipeline_file=pipeline_file),)
            pipeline_file.check_result = CheckResult(pipeline_file.src_path, compliant, compliance_log)


class NonEmptyCheckRunner(BaseCheckRunner):
    def run(self, pipeline_files):
        for pipeline_file in pipeline_files:
            self._logger.info(
                "checking that '{pipeline_file.src_path}' is not empty".format(pipeline_file=pipeline_file))
            compliant = is_nonemptyfile(pipeline_file.src_path)
            compliance_log = () if compliant else ('empty file',)
            pipeline_file.check_result = CheckResult(pipeline_file.src_path, compliant, compliance_log)
