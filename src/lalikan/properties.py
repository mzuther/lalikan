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

import datetime


class BackupProperties:
    """
    Initialise backup properties.

    :param date:
        start time of backup (or "None" to signify that a backup of the
        given level does not exist)
    :type date:
        :py:mod:`datetime.datetime` or None

    :param level:
        backup level (0 to 2)
    :type level:
        integer

    :rtype:
        None

    """
    def __init__(self, date, level):
        if not isinstance(date, datetime.datetime) and date is not None:
            raise ValueError('first parameter must be either '
                             '"datetime.datetime" or "None".')

        self._date = date

        if self.date:
            self._date_string = self._date.strftime('%Y-%m-%d_%H%M')
        else:
            self._date_string = 'None'

        if not level in (0, 1, 2):
            raise ValueError('second parameter must be one of "0", "1" or '
                             '"2".')

        self._level = level
        self._suffix = ('full', 'diff', 'incr')[self._level]


    def __repr__(self):
        """
        String representation of class instance.

        """
        return 'BackupProperties({0}, {1})'.format(self.date, self.suffix)


    def __eq__(self, other):
        """
        Allow comparison of BackupProperties

        """
        assert isinstance(other, BackupProperties), 'cannot compare ' \
            'BackupProperties to type {0}'.format(type(other))

        return self.date_string == other.date_string and \
            self.level == other.level


    def __lt__(self, other):
        """
        Allow comparison (and thus sorting) of BackupProperties

        """
        assert isinstance(other, BackupProperties), 'cannot compare ' \
            'BackupProperties to type {0}'.format(type(other))

        if self.date_string == other.date_string:
            return self.level < other.level
        else:
            return self.date_string < other.date_string


    def __bool__(self):
        raise NotImplementedError()


    @property
    def is_valid(self):
        """
        Attribute: does a backup of the given level exist?

        :returns:
            Boolean stating whether a backup of the given level exists
        :rtype:
            Boolean

        """
        return self.date is not None


    @property
    def date(self):
        """
        Attribute: start time of backup.

        :returns:
            start time of backup (or "None" to signify that a backup of
            the given level does not exist)
        :rtype:
            :py:mod:`datetime.datetime` or None

        """
        return self._date

    @property
    def date_string(self):
        """
        Attribute: start time of backup formatted as string.

        :returns:
            formatted start time of backup (or "None")
        :rtype:
            String

        """
        return self._date_string


    @property
    def level(self):
        """
        Attribute: backup level.

        :returns:
            backup level (0, 1 or 2)
        :rtype:
            integer

        """
        return self._level


    @property
    def suffix(self):
        """
        Attribute: backup suffix.

        :returns:
            backup suffix (one of "full", "diff" or "incr")
        :rtype:
            String

        """
        return self._suffix
