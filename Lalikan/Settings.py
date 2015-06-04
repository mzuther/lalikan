# -*- coding: utf-8 -*-

"""Lalikan
   =======
   Backup scheduler for Disk ARchive (DAR)

   Copyright (c) 2010-2013 Martin Zuther (http://www.mzuther.de/)

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.

   Thank you for using free software!

"""


import configparser
import gettext
import os

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


class Settings:
    """Store user and application settings in one place and make them available.
    """
    def __init__(self, config_filename):
        """Initialise user settings and application information.

        Keyword arguments:
        None

        Return value:
        None

        """
        # common application copyrights and information (only set here, private)
        self._application = 'Lalikan.py'
        self._cmd_line = 'Lalikan'
        self._version = '0.17'
        self._years = '2010-2013'
        self._authors = 'Martin Zuther'
        self._license_short = 'GPL version 3 (or later)'
        self._license_long = """This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

Thank you for using free software!"""
        self._description = _('Backup scheduler for Disk ARchive (DAR)')

        # parse config file
        with open(config_filename, 'rt', encoding='utf-8') as infile:
            self._settings = configparser.ConfigParser(interpolation=None)
            self._settings.read_file(infile)


    def __repr__(self):
        """Return all the contents of the INI file as string.

        Keyword arguments:
        None

        Return value:
        Formatted string containing all options from the INI file

        """
        output = ''

        # output sorted sections
        for section in self.sections():
            output += '\n[{section}]\n'.format(**locals())

            # output sorted options
            for (option, value) in self.items(section):
                output += '{option}: {value}\n'.format(**locals())

        # return the whole thing
        return output.strip()


    def get(self, section, option, allow_empty):
        """Get an application setting.

        Keyword arguments:
        section -- string that specifies the section to be queried
        option -- string that specifies the option to be queried
        allow_empty -- queried string may be empty or option may be
                       non-existant

        Return value:
        String containing the specified application option

        """
        if allow_empty:
            value = self._settings.get(section, option, fallback='')
        else:
            value = self._settings.get(section, option)
            assert value != ''

        return value


    def options(self, section):
        """Get all application option names of a section (sorted).

        Keyword arguments:
        section -- string that specifies the section to be queried

        Return value:
        Tuple containing application option names of the given section

        """
        return tuple(sorted(self._settings.options(section), key=str.lower))


    def items(self, section):
        """Get all application options and their values of a section (sorted).

        Keyword arguments:
        section -- string that specifies the section to be queried

        Return value:
        Tuple containing application options and their values of the
        given section

        """
        items = self._settings.items(section)
        return tuple(sorted(items, key=lambda i: str.lower(i[0])))


    def sections(self):
        """Get all sections (sorted).

        Keyword arguments:
        None

        Return value:
        Tuple containing all section names

        """
        sections = sorted(self._settings.sections(), key=str.lower)

        # move section 'Default' to the top so that the default backup
        # will be run first
        if 'Default' in sections:
            default_item = sections.pop(sections.index('Default'))
            sections.insert(0, default_item)

        return tuple(sections)


    def get_option(self, option):
        """Return application describing option as string.

        Keyword arguments:
        option -- option to query

        Return value:
        Formatted string containing option's value (or None for
        invalid queries)

        """
        # tuple of option names that may be queried (as a security measure)
        valid_option_names = ('application', 'cmd_line', 'version',
                                'years', 'authors', 'license_short',
                                'license_long', 'description')

        if option not in valid_option_names:
            raise ValueError('option "{0}" not found'.format(option))

        return eval('self._{0}'.format(option))


    def get_description(self, long_description, application_name=None):
        """Return application description as string.

        Keyword arguments:
        long_description -- Boolean indication whether to output long
        version of description
        application_name -- optional string holding the application's
        name (defaults to application name set in this file)

        Return value:
        Formatted string containing application description

        """
        if application_name is None:
            application_name = self.get_option('application')

        description = '{application} v{version}'.format(
            application=application_name,
            version=self.get_option('version'))

        description += '\n' + '=' * len(description)

        if long_description:
            description += '\n{description}'.format(
                description=self.get_option('description'))

        return description


    def get_copyrights(self):
        """Return application copyrights as string.

        Keyword arguments:
        None

        Return value:
        Formatted string containing application copyrights

        """
        return '(c) {years} {authors}'.format(
            years=self.get_option('years'),
            authors=self.get_option('authors'))


    def get_license(self, long_description):
        """Return application license as string.

        Keyword arguments:
        long_description -- Boolean indication whether to output long
        version of description

        Return value:
        Formatted string containing application license

        """
        if long_description:
            return self.get_option('license_long')
        else:
            return self.get_option('license_short')


if __name__ == '__main__':
    settings = Settings('/etc/lalikan')

    print()
    print(settings)
    print()
