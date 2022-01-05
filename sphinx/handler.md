
##### input_file
* type: ``str``
* mandatory: True

Path to the file being handled. A non-existent file will cause the handler to exit with an error during the initialise
step. Note: input_file is the only positional argument to the handler.

##### allowed_extensions
* type: ``list``
* mandatory: False

List of allowed extensions for the *input* file. Non-matching input files with cause the handler to exit with an error
during the initialise step.

##### archive_input_file
* type: ``bool``
* mandatory: False

Flags whether the original input file should be uploaded to the archive, the location of which is configured by the
environment configuration. The file will be archived at ARCHIVE_URI/PIPELINE_NAME/BASENAME.

##### archive_path_function
* type: ``str`` or function reference
* mandatory: False

The function used to determine the path the file will be archived as, relative to the ARCHIVE_URI. If absent, the
handler will attempt to use the ``dest_path`` method in the handler itself. If a function is not found, the handler will
exit with an error during the initialise step.

##### celery_task
* type: ``celery.Task``
* mandatory: False

A Celery task object, in order for the handler instance to derive runtime information such as the current task ID etc.

Note: this attribute is not intended to be manipulated directly. If absent (e.g. unit testing), the handler will revert
to having no task information available, and will log output to standard output.

##### check_params
* type: ``dict``
* mandatory: False

A dict containing parameters passed directly to the check step(e.g. compliance checker suites). The structure of the
dict is controlled by the ``CHECK_PARAMS_SCHEMA`` object in the [schema](https://github.com/aodn/python-aodncore/blob/master/aodncore/pipeline/schema.py) module.

##### config
* type: ``LazyConfigManager``
* mandatory: True

A configuration object which the handler uses to retrieve configuration from it's environment. If absent, the handler
will exit with an error during the ``__init__`` method (i.e. will not instantiate).

Note: this attribute is not intended to be manipulated directly. Unit tests inheriting from ``HandlerTestCase`` contain
a pre-prepared config object available as ``self.config``. The ``HandlerTestCase.run_handler`` and
``HandlerTestCase.run_handler_with_exception`` helper methods automatically assign the test config to the handler being
tested.

##### dest_path_function
* type: ``str`` or function reference
* mandatory: False

The function used to determine the path the file will be published as, relative to the UPLOAD_URI configuration item. If
absent, the handler will attempt to use the ``dest_path`` method in the handler itself. If a function is not found, the
handler will exit with an error during the initialise step.

##### exclude_regexes
* type: ``list``
* mandatory: False

A list of regexes which, when combined with ``include_regexes``, determines which files in the collection are assigned
with the ``default_addition_publish_type`` or ``default_deletion_publish_type`` types (depending on whether the file is
an addition or a deletion). Files not matching the inclusion criteria will remain with a ``publish_type`` attribute of
``PipelineFilePublishType.NO_ACTION``, meaning they will be ignored by the publish step.

##### harvest_params
* type: ``dict``
* mandatory: False

A dict containing parameters passed directly to the harvest step(e.g. slice size, undo behaviour). The structure of the
dict is controlled by the ``HARVEST_PARAMS_SCHEMA`` object in the [schema](https://github.com/aodn/python-aodncore/blob/master/aodncore/pipeline/schema.py) module.

##### harvest_type
* type: ``str``
* mandatory: False
* default: 'talend'

String to inform the harvest step factory function which HarvesterRunner implementation to use during the publish step.

Note: 'talend' is the default, but 'csv' is also valid.

##### include_regexes
* type: ``list``
* mandatory: False

See ``exclude_regexes`` for details.

##### notify_params
* type: ``dict``
* mandatory: False

A dict containing parameters passed directly to the notify step(e.g. owner/success/failure notify lists). The structure
of the dict is controlled by the ``NOTIFY_PARAMS_SCHEMA`` object in the [schema](https://github.com/aodn/python-aodncore/blob/master/aodncore/pipeline/schema.py) module.

##### upload_path
* type: ``str``
* mandatory: False

A string attribute to hold the original upload path of the input file.

Note: this is for information purposes only (e.g. to appear in notification templates)

##### resolve_params
* type: ``dict``
* mandatory: False

A dict containing parameters passed directly to the resolve step(e.g. the root path prepended to relative paths in
manifest files). The structure of the dict is controlled by the ``RESOLVE_PARAMS_SCHEMA`` object in the [schema](https://github.com/aodn/python-aodncore/blob/master/aodncore/pipeline/schema.py) module.

##### **kwargs
* type: arbitrary keyword arguments
* mandatory: False

Any additional keyword arguments passed to the handler are ignored by the ``HandlerBase`` base class. This is to leave
open the ability for handler specific params to be passed from the watch configuration to control some arbitrary
handler behaviour, without interfering with the core state machine operation.

#### Runtime attributes

The value of these attributes is determined at runtime, based on a variety of other pieces of information including
input parameters, the result of runtime operations performed during the handler execution and any updates made by 
custom subclasses to the handler state. 
 
##### file_collection
type: ``PipelineFileCollection``

The attribute holding the primary file collection for the handler instance. This holds all of the files that the handler
"knows about", and records the state of all operations attempted against the files.

A ``PipelineFileCollection`` is a custom ordered ``set`` type, designed to hold a ``PipelineFile`` for each known file.

Although typically the collection will refer to the same original files as identified during the resolve step, it is
also possible that the collection be manipulated after this, for example, by adding products generated during the
handler execution.

##### celery_task_id
type: ``str``

Read-only property to retrieve the task ID from the the Celery task (if set).

##### celery_task_name
type: ``str``

Read-only property to retrieve the task ID from the the Celery name (if set).

##### cc_versions
type: ``dict``

Read-only property containing the versions of both the core compliance checker package, and the cc_plugin_imos package.

##### error
type: ``Exception``

Read-only property (updated internally in case of error) containing the ``Exception`` object which caused the handler to
fail, otherwise containing ``None`` for successful execution.

##### error_details
type: ``str``

Property containing the exception message in cause of handler error, optionally containing the full stack trace in case
of system error, or None if no error has occurred.

##### file_checksum
type: ``str``

Read-only property containing the SHA256 file checksum of the input file.

##### file_extension
type: ``str``

Read-only property containing the file extension of the input file.

##### file_type
type: ``FileType`` member

Read-only property containing the file type of the input file.

##### instance_working_directory
type: ``str``

Read-only property containing the path to the unique temporary working directory for this instance

Note: this directory should not be used directly, rather a handler should access one of the subdirectories defined in 
``collection_dir``, ``products_dir`` or ``temp_dir``, as appropriate.

##### notification_results
type: ``NotifyList`` member

Read-only property containing the result of the notification attempts, in a ``NotifyList`` collection. The
``NotifyList`` type is a custom ``set`` object designed to hold ``NotificationRecipient`` objects, each of which
contains the details of the recipient as well as status information relating to notification attempts (e.g. errors).

##### result
type: ``HandlerResult`` member

Read-only property containing the overall result of the handler instance, represented as a member of the
``HandlerResult`` enum.

##### should_notify
type: ``list``

Read-only property containing the list of recipients addresses who *should* be notified. This is generated during the
notify step, and used as an input to the ``NotifyRunner`` instance, which in turn populates the ``notification_results``
attribute.

##### start_time
type: ``datetime``

Read-only property containing the time the handler instance was instantiated. This is simply the output of
``datetime.now()`` called during the ``__init__`` method, primarily useful for use in notification templates.

##### default_addition_publish_type
type: ``PipelineFilePublishType`` member

Property defining the default value of the ``publish_type`` attribute of addition ``PipelineFile`` objects when they are
added to the ``file_collection`` attribute. Defaults to ``PipelineFilePublishType.HARVEST_UPLOAD``, unless overridden in
the ``__init__`` method of handler subclasses.
 
 ##### default_deletion_publish_type
type: ``PipelineFilePublishType`` member

Property defining the default value of the ``publish_type`` attribute of deletion ``PipelineFile`` objects when they are
added to the ``file_collection`` attribute. Defaults to ``PipelineFilePublishType.DELETE_UNHARVEST``, unless overridden
in the ``__init__`` method of handler subclasses.

##### is_archived
type: ``bool``

Property representing whether the input file has been successfully archived.

##### collection_dir
type: ``str``

Read-only property containing the path to the physical files resolved by the resolve step. This is a subdirectory of the
temporary path stored in ``instance_working_directory``.

##### products_dir
type: ``str``

Read-only property containing the path to the directory in which any products produced during the handler execution
should be created. This is a subdirectory of the temporary path stored in ``instance_working_directory``, and is empty
unless modified by a handler subclass.

##### temp_dir
type: ``str``

Read-only property containing the path to the directory in which any miscellaneous temporary files should be created.
This is a subdirectory of the temporary path stored in ``instance_working_directory``, and is empty unless modified by
a handler subclass.
