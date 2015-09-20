# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2015 Dr. Martin Zuther (http://www.mzuther.de/)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Thank you for using free software!

import configparser


class Settings:
    """
    Store user and application settings in one place and make them
    available.

    """
    def __init__(self, config_filename):
        """
        Initialise user settings and application information.

        :rtype:
            None

        """
        # common application copyrights and information (only set here, private)
        self._application = 'Lalikan.py'
        self._cmd_line = 'Lalikan'
        self._version = '0.17'
        self._years = '2010-2015'
        self._authors = 'Dr. Martin Zuther'
        self._license_short = 'Licenced under the GPL version 3 (or later).'
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
        self._description = 'Backup scheduler for Disk ARchive (DAR).'

        # parse config file
        with open(config_filename, 'rt', encoding='utf-8') as infile:
            self._settings = configparser.ConfigParser(interpolation=None)
            self._settings.read_file(infile)


    def __repr__(self):
        """
        Return all the contents of the INI file as string.

        :returns:
            all options from the INI file
        :rtype:
            String

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


    def get(self, section, option_name, allow_empty):
        """
        Get an application setting.

        :param section:
            section to be queried
        :type section:
            String

        :param option_name:
            option to be queried
        :type option_name:
            String

        :param allow_empty:
            queried string may be empty or option may be non-existant
        :type allow_empty:
            Boolean

        :returns:
            specified application option
        :rtype:
            String

        """
        if allow_empty:
            value = self._settings.get(section, option_name, fallback='')
        else:
            value = self._settings.get(section, option_name)
            assert value != ''

        return value


    def options(self, section):
        """
        Get all application option names of a section (sorted).

        :param section:
            section to be queried
        :type section:
            String

        :returns:
             application option names of the given section
        :rtype:
            Tuple

        """
        return tuple(sorted(self._settings.options(section), key=str.lower))


    def items(self, section):
        """
        Get all application options and their values of a section (sorted).

        :param section:
            section to be queried
        :type section:
            String

        :returns:
            application options and their values of the given section
        :rtype:
            Tuple

        """
        items = self._settings.items(section)
        return tuple(sorted(items, key=lambda i: str.lower(i[0])))


    def sections(self):
        """
        Get all sections (sorted).

        :returns:
             all section names
        :rtype:
            Tuple

        """
        sections = sorted(self._settings.sections(), key=str.lower)

        # move section 'Default' to the top so that the default backup
        # will be run first
        if 'Default' in sections:
            default_item = sections.pop(sections.index('Default'))
            sections.insert(0, default_item)

        return tuple(sections)


    def get_option(self, option):
        """
        Return application describing option as string.

        :param option:
            option to query
        :type option:
            String

        :returns:
            the option's value
        :rtype:
            String (or None for invalid queries)

        """
        # tuple of option names that may be queried (as a security measure)
        valid_option_names = ('application', 'cmd_line', 'version',
                                'years', 'authors', 'license_short',
                                'license_long', 'description')

        if option not in valid_option_names:
            raise ValueError('option "{0}" not found'.format(option))

        return eval('self._{0}'.format(option))


    def get_name_and_version(self, application_name=None):
        """
        Return application version as string.

        :param application_name:
            application name (defaults to name set in this file)
        :type application_name:
            String

        :returns:
            application version
        :rtype:
            String

        """
        if application_name is None:
            application_name = self.get_option('application')

        version = '{application} {version}'.format(
            application=application_name,
            version=self.get_option('version'))

        return version


    def get_description(self):
        """
        Return application description as string.

        :returns:
            application description
        :rtype:
            String

        """
        return self.get_option('description')


    def get_copyrights(self):
        """
        Return application copyrights as string.

        :returns:
            application copyrights
        :rtype:
            String

        """
        return 'Copyright (c) {years} {authors}'.format(
            years=self.get_option('years'),
            authors=self.get_option('authors'))


    def get_license(self, long_description):
        """
        Return application license as string.

        :param long_description:
            output long version of description
        :type long_description:
            Boolean

        :returns:
            application license
        :rtype:
            String

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
