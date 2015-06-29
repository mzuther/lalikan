# Lalikan
# =======
# Backup scheduler for Disk ARchive (DAR)
#
# Copyright (c) 2010-2015 Martin Zuther (http://www.mzuther.de/)
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
import gettext
import os
import re
import sys

import lalikan.utilities

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


class BackupDatabase:
    """
    Initialise database.

    :param settings:
        backup settings and application information
    :type settings:
        lalikan.settings

    :param section:
        section of backup settings to use (such as *Workstation* or *Server*)
    :type section:
        String

    :rtype:
        None

    """
    def __init__(self, settings, section):
        # Lalikan settings (such as backup directory)
        self._settings = settings

        # backup section (such as "Workstation" or "Server")
        self._section = section

        # valid backup levels
        #
        # full: complete backup, contains everything
        # diff: differential, contains all changes since last full backup
        # incr: incremental, contains all changes since last backup
        self._backup_levels = ('full', 'diff', 'incr')

        # backup file name postfixes time for all backup levels
        self._postfixes = ('full', 'diff', 'incr')


    def _get_option(self, option_name, allow_empty=False):
        """
        Query current backup settings for specified option.

        :param option_name:
            name of option to query
        :type option_name:
            String

        :param allow_empty:
            return value may be empty (or option may be non-existant)
        :type allow_empty:
            Boolean

        :returns:
            the option's value
        :rtype:
            String

        """
        return self._settings.get(self._section, option_name, allow_empty)


    @property
    def dar_path(self):
        """
        Attribute: file path to dar executable.

        :returns:
            file path to dar executable
        :rtype:
            String

        """
        return self._get_option('dar_path')


    @property
    def dar_options(self):
        """
        Attribute: DAR command line options

        :returns:
            DAR command line options
        :rtype:
            String

        """
        return self._get_option('dar_options', True)


    @property
    def backup_directory(self):
        """
        Attribute: file path for backup directory.

        :returns:
            file path for backup directory
        :rtype:
            String

        """
        return self._get_option('backup_directory')


    @property
    def interval_full(self):
        """
        Attribute: interval for "full" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self._get_option('interval_full')
        return float(interval)


    @property
    def interval_diff(self):
        """
        Attribute: interval for "diff" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self._get_option('interval_diff')
        return float(interval)


    @property
    def interval_incr(self):
        """
        Attribute: interval for "incr" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self._get_option('interval_incr')
        return float(interval)


    @property
    def postfix_full(self):
        """
        Attribute: file postfix for "full" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return 'full'


    @property
    def postfix_diff(self):
        """
        Attribute: file postfix for "diff" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return 'diff'


    @property
    def postfix_incr(self):
        """
        Attribute: file postfix for "incr" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return 'incr'


    @property
    @lalikan.utilities.Memoized
    def start_time(self):
        """
        Attribute: start time of first backup (such as "2012-12-31_2100").
        This argument is parsed using :py:meth:`date_format`.

        :returns:
            start time of first backup
        :rtype:
            :py:mod:`datetime.datetime`

        """
        start_time = self._get_option('start_time')
        return datetime.datetime.strptime(start_time, self.date_format)


    @property
    def date_format(self):
        """
        Attribute: date format string.  Please see
        :py:meth:`datetime.datetime.strftime` for more information.

        :returns:
            date format string
        :rtype:
            String

        """
        return '%Y-%m-%d_%H%M'


    @property
    def backup_regex(self):
        """
        Attribute: regular expression for matching backup directory names.

        :returns:
            compiled regular expression object
        :rtype:
            :py:mod:`re`

        """
        # regular expression for valid backup dates
        date_regex = '[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}'

        # regular expression for valid backup postfixes
        postfix_regex = '|'.join(self._postfixes)

        # compile regular expression for valid backup directory names
        regex = re.compile('^({0})-({1})$'.format(date_regex, postfix_regex))

        return regex


    @property
    def pre_run_command(self):
        """
        Attribute: command that is executed in the shell before the backup
        is started.

        :returns:
            shell command
        :rtype:
            String

        """
        return self._get_option('command_pre_run', True)


    @property
    def post_run_command(self):
        """
        Attribute: command that is executed in the shell after the backup
        hss finished.

        :returns:
            shell command
        :rtype:
            String

        """
        return self._get_option('command_post_run', True)


    def _accepted_backup_levels(self, backup_level):
        """
        Get backup levels that will be accepted as substitute for given
        backup level.

        :param backup_level:
            backup level
        :type backup_level:
            String

        :returns:
            accepted backup levels
        :rtype:
            list

        """
        accepted_levels = []

        # count all backup levels above given backup level as valid
        for level in self._backup_levels:
            accepted_levels.append(level)
            if level == backup_level:
                break

        return accepted_levels


    def _check_backup_level(self, backup_level):
        """
        Checks whether the specified backup level (such as "full")
        actually exists.

        :param backup_level:
            backup level
        :type backup_level:
            String

        :raises: :py:class:`ValueError`

        :rtype:
            None

        """
        if backup_level not in self._backup_levels:
            raise ValueError(
                'wrong backup level given ("{0}")'.format(backup_level))


    def _fill_schedule(self, schedule, backup_level, interval):
        """
        Fill schedule with backups of a certain backup level using a given
        interval.

        :param schedule:
            existing schedule containing times and corresponding
            backup levels
        :type schedule:
            list of tuple(:py:mod:`datetime.datetime`, String)

        :param backup_level:
            backup level ("diff" or "incr")
        :type backup_level:
            String

        :param interval:
            backup interval
        :type interval:
            :py:mod:`datetime.timedelta`

        :returns:
            updated schedule containing times and corresponding backup
            levels
        :rtype:
            list of tuple(:py:mod:`datetime.datetime`, String)

        """
        # temporary storage for newly scheduled backups
        temp_schedule = []

        # loop over existing schedule, ignoring the last entry
        for n in range(len(schedule) - 1):
            # start with current scheduled backup
            start_time = schedule[n][0]

            # end with next scheduled backup
            end_time = schedule[n + 1][0]

            # add one interval so that existing and new backup do not
            # overlap
            start_time += interval

            # calculate backup start times
            while start_time < end_time:
                # store new backup
                new_backup = (start_time, backup_level)
                temp_schedule.append(new_backup)

                # move on
                start_time += interval

        # consolidate backup start times
        schedule.extend(temp_schedule)

        # sort consolidated backup start times by date
        schedule = sorted(schedule, key=lambda k: k[0])

        # return updated schedule
        return schedule


    @lalikan.utilities.Memoized
    def calculate_backup_schedule(self, point_in_time):
        """
        Calculate backup schedule, starting from the "full" backup prior to
        the given point in time and ending with the next one.

        :param point_in_time:
            given point in time
        :type point_in_time:
            :py:mod:`datetime.datetime`

        :returns:
            backup schedule containing times and corresponding backup
            levels
        :rtype:
            list of tuple(:py:mod:`datetime.datetime`, String)

        """
        # prepare backup intervals
        delta_full = datetime.timedelta(self.interval_full)
        delta_diff = datetime.timedelta(self.interval_diff)
        delta_incr = datetime.timedelta(self.interval_incr)

        # calculate first "full" backup after the given date
        current_start_time = self.start_time
        while current_start_time <= point_in_time:
            current_start_time += delta_full

        # store upcoming "full" backup
        new_backup = (current_start_time, 'full')
        schedule = [new_backup]

        # calculate previous "full" backup
        current_start_time -= delta_full

        # store previous "full" backup (if valid)
        if current_start_time >= self.start_time:
            new_backup = (current_start_time, 'full')
            schedule.insert(0, new_backup)

        # found "full" backup prior to given date
        if len(schedule) > 1:
            # fill schedule with "diff" backups
            schedule = self._fill_schedule(schedule, 'diff', delta_diff)

            # fill schedule with "incr" backups
            schedule = self._fill_schedule(schedule, 'incr', delta_incr)

        # return schedule
        return schedule


    @lalikan.utilities.Memoized
    def _current_scheduled_backup(self, point_in_time, backup_level):
        """
        Find current scheduled backup for a given backup level.

        :param point_in_time:
            given point in time
        :type point_in_time:
            :py:mod:`datetime.datetime`

        :param backup_level:
            backup level
        :type backup_level:
            String

        :returns:
            time and postfix of current scheduled backup
        :rtype:
            tuple containing datetime.datetime and String (or None)

        """
        # find scheduled backups
        schedule = self.calculate_backup_schedule(point_in_time)

        # reverse order of scheduled backups
        reversed_schedule = reversed(schedule)

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # loop over reversed schedule
        for scheduled_backup in reversed_schedule:
            # we found the current scheduled backup when it matches
            # any of the accepted levels ...
            if scheduled_backup[1] in accepted_levels:
                # ... and it doesn't lie in the future
                if scheduled_backup[0] <= point_in_time:
                    return scheduled_backup

        # no matching scheduled backup found
        return None


    def find_existing_backups(self, prior_to=None):
        """
        Find existing backups.  May be limited to backups prior to (or
        exactly at) a given point in time.

        :param prior_to:
            given point in time
        :type prior_to:
            None or :py:mod:`datetime.datetime`

        :returns:
            existing backups (timestamp and backup level)
        :rtype:
            tuple(String, String)

        """
        # look for existing backups
        existing_backups = []

        # get subdirectories in backup directory
        directories = [f for f in os.listdir(self.backup_directory)
                       if os.path.isdir(os.path.join(self.backup_directory, f))]

        # loop over subdirectories
        for dirname in directories:
            # check whether the path matches the regular expression
            # for backup directory names
            match = self.backup_regex.match(dirname)

            # path name matches regular expression
            if match is not None:
                # extract path elements
                timestamp, backup_level = match.groups()

                # convert to absolute path name
                full_path = os.path.join(self.backup_directory, dirname)

                # valid backups contain a backup catalog; prepare
                # search for this catalog
                catalog_name = '{0}-catalog.01.dar'.format(timestamp)
                catalog_path = os.path.join(full_path, catalog_name)

                # catalog file exists
                if os.path.isfile(catalog_path):
                    # regard path as valid backup
                    existing_backups.append((timestamp, backup_level))

        # sort backups by path name
        existing_backups.sort()

        # optionally filter backups by date
        if prior_to:
            temp = []

            # loop over found backups
            for backup in existing_backups:
                # convert timestamp to "datetime" object
                timestamp = backup[0]
                backup_date = datetime.datetime.strptime(
                    timestamp, self.date_format)

                # keep backups prior to (or exactly at) given date
                if backup_date <= prior_to:
                    temp.append(backup)

            # store filtered backups
            existing_backups = temp

        #return result
        return existing_backups


    @lalikan.utilities.Memoized
    def last_existing_backup(self, point_in_time, backup_level):
        # assert valid backup level
        self._check_backup_level(backup_level)

        # find existing backups
        existing_backups = self.find_existing_backups(point_in_time)

        # no backups were found
        if not existing_backups:
            return None

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # backwards loop over found backups
        for n in range(len(existing_backups), 0, -1):
            # sequences start at index zero
            index = n - 1

            # we found the last backup when the current one matches
            # any of the accepted levels
            if existing_backups[index][1] in accepted_levels:
                last_existing = datetime.datetime.strptime(
                    existing_backups[index][0], self.date_format)
                backup_level = existing_backups[index][1]

                return (last_existing, backup_level)

        assert False, "this part of the code should never be reached!"


    @lalikan.utilities.Memoized
    def last_scheduled_backup(self, point_in_time, backup_level):
        # assert valid backup level
        self._check_backup_level(backup_level)

        # get last existing backup of given (or lower) level
        last_existing = self.last_existing_backup(point_in_time, backup_level)

        # get date of last existing backup
        if last_existing is not None:
            last_existing = last_existing[0]
        # if this fails, use date of the Epoch so that "last_existing"
        # can be compared to "datetime" objects
        else:
            last_existing = datetime.datetime(1970, 1, 1)

        if backup_level == 'full':
            # see whether we need a "full" backup
            full = self._current_scheduled_backup(point_in_time, 'full')
            return full
        elif backup_level == 'diff':
            # do we need a "full" backup?
            full = self._current_scheduled_backup(point_in_time, 'full')
            if (full is not None) and (last_existing < full[0]):
                return full

            # otherwise, see whether we need a "diff" backup
            diff = self._current_scheduled_backup(point_in_time, 'diff')

            return diff
        elif backup_level == 'incr':
            # do we need a "full" backup?
            full = self._current_scheduled_backup(point_in_time, 'full')
            if (full is not None) and (last_existing < full[0]):
                return full

            # do we need a "diff" backup?
            diff = self._current_scheduled_backup(point_in_time, 'diff')

            if (diff is not None) and (last_existing < diff[0]):
                return diff

            # otherwise, see whether we need an "incr" backup
            incr = self._current_scheduled_backup(point_in_time, 'incr')
            return incr


    @lalikan.utilities.Memoized
    def next_scheduled_backup(self, point_in_time, backup_level):
        # assert valid backup level
        self._check_backup_level(backup_level)

        # find scheduled backups
        scheduled_backups = self.calculate_backup_schedule(point_in_time)

        # no backups were scheduled
        if not scheduled_backups:
            assert False, "this part of the code should never be reached!"

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # loop over scheduled backups
        for index in range(len(scheduled_backups)):
            # we found the next scheduled backup when the current one
            # matches any of the accepted levels ...
            if scheduled_backups[index][1] in accepted_levels:
                next_backup = scheduled_backups[index][0]
                backup_level = scheduled_backups[index][1]

                # ... and it lies in the future
                if next_backup > point_in_time:
                    return (next_backup, backup_level)

        assert False, "this part of the code should never be reached!"


    @lalikan.utilities.Memoized
    def days_overdue(self, point_in_time, backup_level):
        last_scheduled = self.last_scheduled_backup(point_in_time, backup_level)
        last_existing = self.last_existing_backup(point_in_time, backup_level)

        # no scheduled backup lies in the past
        if last_scheduled is None:
            next_scheduled = self.next_scheduled_backup(
                point_in_time, backup_level)
            calculation_base = next_scheduled[0]
        # no backup of this level has been executed yet
        elif last_existing is None:
            calculation_base = last_scheduled[0]
        # last backup of this level is older than scheduled backup
        elif last_existing[0] < last_scheduled[0]:
            calculation_base = last_scheduled[0]
        # last executed backup is current
        else:
            next_scheduled = self.next_scheduled_backup(
                point_in_time, backup_level)
            calculation_base = next_scheduled[0]

        # calculate fractional days since/until scheduled backup
        days_overdue = point_in_time - calculation_base
        days_overdue = days_overdue / datetime.timedelta(days=1)

        # negative numbers:  days since when backup level is overdue
        # positive numbers:  days until the next scheduled backup
        return days_overdue


    @lalikan.utilities.Memoized
    def backup_needed(self, point_in_time, force_backup):
        """
        Find out whether a backup is necessary for a given point in time.

        :param point_in_time:
            given point in time
        :type point_in_time:
            :py:mod:`datetime.datetime`

        :param force_backup:
            **True** forces a backup
        :type force_backup:
            Boolean

        :returns:
            backup level when backup is needed, **None** otherwise
        :rtype:
            String or None

        """
        # check necessity of a backup for all backup levels
        for backup_level in self._backup_levels:
            # a backup of this level is necessary
            if self.days_overdue(point_in_time, backup_level) >= 0.0:
                return backup_level

        # force backup, but only after schedule begins
        if force_backup and point_in_time >= self.start_time:
            return 'forced'
        # no backup necessary
        else:
            return None


    # method can't be memoized, since results depend on current
    # working directory!
    def sanitise_path(self, path_name):
        """
        Return a normalised absolutised version of the specified path name.
        On Windows, the path name will also be converted to something that
        Cygwin understands (such as "/cygdrive/c/path/to/dar").

        :param path_name:
            path name
        :type path_name:
            String

        :raises: :py:class:`ValueError`

        :returns:
            sanitised path name
        :rtype:
            String

        """
        # normalise and absolutise path name
        path_name = os.path.abspath(path_name)

        # assert that path has a length
        if not path_name:
            raise ValueError('path name is empty')

        # Windows: DAR uses Cygwin internally
        if sys.platform in ('win32', 'cygwin'):
            # extract drive from path name
            drive, tail = os.path.splitdrive(path_name)

            # extract drive letter
            drive = drive[0]

            # convert to lower space
            drive = drive.lower()

            # remove heading path separator from remaining path
            if tail.startswith(os.sep):
                tail = tail[len(os.sep):]

            # turn path to something like "/cygdrive/c/path/to/dar"
            path_name = os.path.join(os.sep, 'cygdrive', drive, tail)

            # Cygwin uses "/" as path separator
            path_name = path_name.replace(os.sep, '/')

        return path_name


    # ----- OLD CODE -----

    def get_backup_reference(self, backup_level):
        # assert valid backup level
        self._check_backup_level(backup_level)

        if backup_level == 'full':
            reference_base = 'none'
            reference_option = ''
        elif backup_level == 'diff':
            reference_base = self.name_of_last_backup('full')
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{0}-catalog'.format(reference_timestamp)

            full_path = os.path.join(self.backup_directory,
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)
        elif backup_level == 'incr':
            last_full = self.days_since_last_backup('full')
            last_diff = self.days_since_last_backup('diff')
            last_incr = self.days_since_last_backup('incr')

            newest_backup = 'full'
            newest_age = last_full

            if (last_diff >= 0) and (last_diff < newest_age):
                newest_backup = 'diff'
                newest_age = last_diff

            if (last_incr >= 0) and (last_incr < newest_age):
                newest_backup = 'incr'
                newest_age = last_incr

            reference_base = self.name_of_last_backup(newest_backup)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{0}-catalog'.format(reference_timestamp)

            full_path = os.path.join(self.backup_directory,
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)

        return (reference_base, reference_option)
