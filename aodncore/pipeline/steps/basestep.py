import abc

__all__ = [
    'AbstractCollectionStepRunner',
    'AbstractNotifyRunner',
    'AbstractResolveRunner'
]


class BaseStepRunner(object):
    def __init__(self, config, logger):
        self._config = config
        self._logger = logger


class AbstractCollectionStepRunner(BaseStepRunner):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self, pipeline_files):
        pass


class AbstractNotifyRunner(BaseStepRunner):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self, notify_list):
        pass


class AbstractResolveRunner(BaseStepRunner):
    """A ResolveRunner is responsible for resolving an input file by:

        1a) when input_file represents multiple files (e.g. ZIP archive, manifest file), the 'child' files are
            copied/extracted/downloaded to the output_dir using the appropriate mechanism
        1b) when input_file represents a single file (e.g. NC file), the file itself is copied to the output_dir

        2) the file(s) are added to a PipelineFileCollection set for use by the rest of the handler.

        The '__init__' method is supplied with the input file and the output_dir. The abstract 'run' method performs
        the steps above in the most appropriate/efficient way, provided that when it completes:

        1) the output_dir contains all of the files being handled in this handler instance
        2) the run method returns a PipelineFileCollection instance populated with all of these files

        This means the rest of the handler code has no further need to be aware of the source of the files, and files
        may then be processed in a generic way.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod  # pragma: no cover
    def run(self):
        pass
