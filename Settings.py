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
    def __init__(self):
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

        # set INI file path
        if os.name == 'posix':
            self._config_file_path = os.path.expanduser('/etc/lalikan')
        elif os.name == 'nt':
            # for the lack of a good place, look for the configuration
            # file in the application's directory
            self._config_file_path = os.path.expanduser('lalikan.conf')
        else:
            assert False

        # parse config file
        with open(self._config_file_path, 'rt', encoding='utf-8') as infile:
            self._settings = configparser.ConfigParser(interpolation=None)
            self._settings.read_file(infile)


    def __repr__(self):
        """Return all the contents of the INI file as string.

        Keyword arguments:
        None

        Return value:
        Formatted string containing all settings from the INI file

        """
        output = ''

        # output sorted sections
        for section in self.sections():
            output += '\n[{section}]\n'.format(**locals())

            # output sorted settings
            for (variable, setting) in self.items(section):
                output += '{variable}: {setting}\n'.format(**locals())

        # return the whole thing
        return output.strip()


    def get(self, section, setting, allow_empty):
        """Get an application setting.

        Keyword arguments:
        section -- string that specifies the section to be queried
        setting -- string that specifies the setting to be queried
        allow_empty -- queried string may be empty or setting may be
                       non-existant

        Return value:
        String containing the specified application setting

        """
        if allow_empty:
            value = self._settings.get(section, setting, fallback='')
        else:
            value = self._settings.get(section, setting)
            assert value != ''

        return value


    def items(self, section):
        """Get all application setting names of a section (sorted).

        Keyword arguments:
        section -- string that specifies the section to be queried

        Return value:
        List containing application setting names of the given section

        """
        return sorted(self._settings.items(section))


    def sections(self):
        """Get all sections (sorted).

        Keyword arguments:
        None

        Return value:
        List containing all section names

        """
        sections = sorted(self._settings.sections())

        # move section 'Default' to the top so that the default backup
        # will be run first
        if 'Default' in sections:
            default_item = sections.pop(sections.index('Default'))
            sections.insert(0, default_item)

        return sections


    def get_variable(self, variable):
        """Return application describing variable as string.

        Keyword arguments:
        variable -- variable to query

        Return value:
        Formatted string containing variable's value (or None for
        invalid queries)

        """
        # list of variable names that may be queried (as a security measure)
        valid_variable_names = ('application', 'cmd_line', 'version',
                                'years', 'authors', 'license_short',
                                'license_long', 'description')

        if variable not in valid_variable_names:
            raise ValueError('variable "{0}" not found'.format(variable))

        return eval('self._{0}'.format(variable))


    def get_description(self, long_description):
        """Return application description as string.

        Keyword arguments:
        long_description -- Boolean indication whether to output long
        version of description

        Return value:
        Formatted string containing application description

        """
        description = '{application} v{version}'.format(
            application=self.get_variable('application'),
            version=self.get_variable('version'))

        description += '\n' + '=' * len(description)

        if long_description:
            description += '\n{description}'.format(
                description=self.get_variable('description'))

        return description


    def get_copyrights(self):
        """Return application copyrights as string.

        Keyword arguments:
        None

        Return value:
        Formatted string containing application copyrights

        """
        return '(c) {years} {authors}'.format(
            years=self.get_variable('years'),
            authors=self.get_variable('authors'))


    def get_license(self, long_description):
        """Return application license as string.

        Keyword arguments:
        long_description -- Boolean indication whether to output long
        version of description

        Return value:
        Formatted string containing application license

        """
        if long_description:
            return self.get_variable('license_long')
        else:
            return self.get_variable('license_short')


if __name__ == '__main__':
    settings = Settings()

    print()
    print(settings)
    print()
