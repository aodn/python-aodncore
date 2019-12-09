"""FileClassifier - Generic class for working out the destination path of
a file to be published. The idea is to define the common functionality
here, then create subclasses to customise for each specific incoming
handler. 

Expected use::

    class MyFileClassifier(FileClassifier):
        def dest_path(self, input_file):
            path = <case-specific logic> 
            ...
            return path

    try:
        dest_path = MyFileClassifier.dest_path(input_file)
    except FileClassifierException, e:
        print >>sys.stderr, e
        raise

    print dest_path

"""

import os
import re

from datetime import datetime

from netCDF4 import Dataset

from .exceptions import InvalidFileFormatError, InvalidFileNameError, InvalidFileContentError


class FileClassifier(object):
    """Base class for working out where a file should be published."""

    @classmethod
    def _get_file_name_fields(cls, input_file, min_fields=6):
        """Return the '_'-separated fields in the file name as a list.
        Raise an exception if the number of fields is less than min_fields.
        """
        # trim off dirs & extention
        basename = os.path.basename(input_file)
        just_the_name = re.sub(r'.\w*$', '', basename)

        fields = just_the_name.split('_')
        if len(fields) < min_fields:
            raise InvalidFileNameError(
                "'{name}' has less than {nfld:d} fields in file name.".format(name=input_file, nfld=min_fields)
            )
        return fields

    @classmethod
    def _get_facility(cls, input_file, check_sub=True):
        """Get the facility/sub-facility from the file name and return as a
        tuple ('facility', 'sub-facility'). Raise exception if no
        sub-facility is present, unless check_sub is False.

        """
        name_field = cls._get_file_name_fields(input_file, min_fields=2)
        fac_subfac = name_field[1].split('-')
        if check_sub and len(fac_subfac) < 2:
            raise InvalidFileNameError("Missing sub-facility in file name '{name}'".format(name=input_file))
        return tuple(fac_subfac)

    @classmethod
    def _open_nc_file(cls, file_path):
        """Open a NetCDF file for reading"""
        try:
            return Dataset(file_path, mode='r')
        except Exception:
            raise InvalidFileFormatError("Could not open NetCDF file '{path}'.".format(path=file_path))

    @classmethod
    def _get_nc_att(cls, file_path, att_name, default=None, time_format=None):
        """Return the value of a global attribute from a NetCDF file. If a list of attribute
        names is given, a list of values is returned. Unless a default value other than None
        is given, a missing attribute raises an exception.

        If time_format is not None, the value of the attribute is converted into a datetime
        object using the given format. If this fails an error is raised. If time_format=True,
        use the format required by the IMOS conventions.

        """
        dataset = cls._open_nc_file(file_path)

        if isinstance(att_name, list):
            att_list = att_name
        else:
            att_list = [att_name]
        values = []

        if time_format is True:
            time_format = '%Y-%m-%dT%H:%M:%SZ'

        for att in att_list:
            if not hasattr(dataset, att):
                if default is None:
                    raise InvalidFileContentError(
                        "File '{path}' has no attribute '{att}'".format(path=file_path, att=att)
                    )
                else:
                    values.append(default)
                    continue

            val = getattr(dataset, att)
            if time_format:
                try:
                    val = datetime.strptime(val, time_format)
                except ValueError:
                    raise InvalidFileContentError(
                        "Could not parse attribute {att}='{val}' as a datetime"
                        " (file '{file_path}')".format(att=att, val=val, file_path=file_path)
                    )

            values.append(val)
        dataset.close()

        if isinstance(att_name, list):
            return values
        return values[0]

    @classmethod
    def _get_site_code(cls, input_file):
        """Return the site_code attribute of the input_file"""
        return cls._get_nc_att(input_file, 'site_code')

    @classmethod
    def _get_variable_names(cls, input_file):
        """Return a list of the variable names in the file."""
        dataset = cls._open_nc_file(input_file)
        names = list(dataset.variables.keys())
        dataset.close()
        return names

    @classmethod
    def _make_path(cls, dir_list):
        """Create a path from a list of directory names, making sure the
         result is a plain ascii string, not unicode (which could
         happen if some of the components of dir_list come from NetCDF
         file attributes).

        """
        for i in range(len(dir_list)):
            dir_list[i] = str(dir_list[i])
        return os.path.join(*dir_list)
