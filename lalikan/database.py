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
        # full:          complete backup, contains everything
        # differential:  contains all changes since last full backup
        # incremental:   contains all changes since last backup
        self._backup_levels = ('full', 'differential', 'incremental')

        # backup file name postfixes time for all backup levels
        self._postfixes = {
            'full': 'full',
            'differential': 'diff',
            'incremental': 'incr'
        }


    def get_option(self, option_name, allow_empty=False):
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
        return self.get_option('dar_path')


    @property
    def dar_options(self):
        """
        Attribute: DAR command line options

        :returns:
            DAR command line options
        :rtype:
            String

        """
        return self.get_option('dar_options', True)


    @property
    @lalikan.utilities.Memoized
    def backup_directory(self):
        """
        Attribute: file path for backup directory.

        :returns:
            file path for backup directory
        :rtype:
            String

        """
        return self.get_option('backup_directory')


    @property
    @lalikan.utilities.Memoized
    def interval_full(self):
        """
        Attribute: interval for "full" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self.get_option('interval_full')
        return float(interval)


    @property
    @lalikan.utilities.Memoized
    def interval_diff(self):
        """
        Attribute: interval for "differential" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self.get_option('interval_differential')
        return float(interval)


    @property
    @lalikan.utilities.Memoized
    def interval_incr(self):
        """
        Attribute: interval for "incremental" backups.

        :returns:
            backup interval in days
        :rtype:
            float

        """
        interval = self.get_option('interval_incremental')
        return float(interval)


    @property
    @lalikan.utilities.Memoized
    def postfix_full(self):
        """
        Attribute: file postfix for "full" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes['full']


    @property
    @lalikan.utilities.Memoized
    def postfix_diff(self):
        """
        Attribute: file postfix for "differential" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes['differential']


    @property
    @lalikan.utilities.Memoized
    def postfix_incr(self):
        """
        Attribute: file postfix for "incremental" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes['incremental']


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
        start_time = self.get_option('start_time')
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
    def date_regex(self):
        """
        Attribute: regular expression for matching backup dates.

        :returns:
            regular expression
        :rtype:
            String

        """
        return '[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{4}'


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
        return self.get_option('command_pre_run', True)


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
        return self.get_option('command_post_run', True)


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
        if backup_level not in self._postfixes:
            raise ValueError(
                'wrong backup level given ("{0}")'.format(backup_level))


    @lalikan.utilities.Memoized
    def calculate_backup_schedule(self, now):
        # initialise dict to hold scheduled backup times
        start_times = {}
        for postfix in self._postfixes.values():
            start_times[postfix] = []

        # initialise variables to calculate all scheduled "full"
        # backups until given date
        current_start_time = self.start_time
        backup_end_time = now
        delta = datetime.timedelta(self.interval_full)

        # calculate all scheduled "full" backups until given date
        while current_start_time <= backup_end_time:
            start_times['full'].append(current_start_time)
            current_start_time += delta

        # calculate upcoming "full" backup
        start_times['full'].append(current_start_time)

        # found one or more scheduled "full" backups prior to given
        # date
        if len(start_times['full']) > 1:
            # skip all scheduled backups except for last "full" backup
            # and upcoming "full" backup
            start_times['full'] = start_times['full'][-2:]

            # copy limits for "differential" backups from "full"
            # backups (will be removed later on)
            start_times['diff'] = start_times['full'][:]

            # initialise variables to calculate all scheduled
            # "differential" backups until given date
            current_start_time = start_times['diff'][0]
            backup_end_time = start_times['diff'][-1]
            delta = datetime.timedelta(self.interval_diff)

            # move one backup cycle from last scheduled "full" backup
            current_start_time += delta

            # calculate all scheduled "differential" backups between
            # last scheduled "full" backup and upcoming "full" backup
            while current_start_time < backup_end_time:
                # insert values in the list's middle
                start_times['diff'].insert(-1, current_start_time)
                current_start_time += delta

            # calculate all scheduled "incremental" backups between
            # scheduled "full" and "differential" backups
            for n in range(len(start_times['diff'][:-1])):
                # initialise variables to calculate all scheduled
                # "incremental" backups until given date
                current_start_time = start_times['diff'][n]
                backup_end_time = start_times['diff'][n + 1]
                delta = datetime.timedelta(self.interval_incr)

                # move one backup cycle from last scheduled "full" or
                # "differential" backup
                current_start_time += delta

                # calculate all scheduled "incremental" backups
                # between scheduled "full" or "incremental" backups
                while current_start_time < backup_end_time:
                    start_times['incr'].append(current_start_time)
                    current_start_time += delta

            # remove "full" backup limits from "differential" backups
            start_times['diff'] = start_times['diff'][1:-1]

        # consolidate backup start times into a single list
        consolidation = []
        for postfix in self._postfixes.values():
            for start_time in start_times[postfix]:
                consolidation.append((start_time, postfix))

        # sort consolidated backup start times by date
        backup_schedule = sorted(consolidation, key=lambda k: k[0])

        # make consolidated result read-only
        backup_schedule = tuple(backup_schedule)

        # return consolidated backup start times
        return backup_schedule


    @lalikan.utilities.Memoized
    def __last_scheduled_backup(self, backup_level, now):
        # find scheduled backups
        scheduled_backups = self.calculate_backup_schedule(now)

        # no backups were scheduled
        if not scheduled_backups:
            return None

        # only "full" backups count as "full" backup
        if backup_level == 'full':
            accepted_levels = ('full', )
        # both "full" and "differential" backups count as
        # "differential" backup
        elif backup_level == 'differential':
            accepted_levels = ('full', 'diff')
        # all backup levels count as "incremental" backup
        elif backup_level == 'incremental':
            accepted_levels = ('full', 'diff', 'incr')

        # backwards loop over scheduled backups
        for n in range(len(scheduled_backups), 0, -1):
            # sequences start at index zero
            index = n - 1

            # we found the last scheduled backup when the current one
            # matches any of the accepted levels ...
            if scheduled_backups[index][1] in accepted_levels:
                last_scheduled = scheduled_backups[index][0]
                backup_level = scheduled_backups[index][1]

                # ... and it doesn't lie in the future
                if last_scheduled <= now:
                    return (last_scheduled, backup_level)

        # no matching scheduled backup found
        return None


    @lalikan.utilities.Memoized
    def last_scheduled_backup(self, backup_level, now):
        self._check_backup_level(backup_level)

        # get last existing backup of given (or lower) level
        last_existing = self.last_existing_backup(backup_level, now)

        # get date of last existing backup
        if last_existing is not None:
            last_existing = last_existing[0]
        # if this fails, use date of the Epoch so that "last_existing"
        # can be compared to "datetime" objects
        else:
            last_existing = datetime.datetime(1970, 1, 1)

        if backup_level == 'full':
            # see whether we need a "full" backup
            full = self.__last_scheduled_backup('full', now)
            return full
        elif backup_level == 'differential':
            # do we need a "full" backup?
            full = self.__last_scheduled_backup('full', now)
            if (full is not None) and (last_existing < full[0]):
                return full

            # otherwise, see whether we need a "differential" backup
            diff = self.__last_scheduled_backup('differential', now)
            return diff
        elif backup_level == 'incremental':
            # do we need a "full" backup?
            full = self.__last_scheduled_backup('full', now)
            if (full is not None) and (last_existing < full[0]):
                return full

            # do we need a "differential" backup?
            diff = self.__last_scheduled_backup('differential', now)
            if (diff is not None) and (last_existing < diff[0]):
                return diff

            # otherwise, see whether we need an "incremental" backup
            incr = self.__last_scheduled_backup('incremental', now)
            return incr


    @lalikan.utilities.Memoized
    def next_scheduled_backup(self, backup_level, now):
        self._check_backup_level(backup_level)

        # find scheduled backups
        scheduled_backups = self.calculate_backup_schedule(now)

        # no backups were scheduled
        if not scheduled_backups:
            assert False, "this part of the code should never be reached!"

        # only "full" backups count as "full" backup
        if backup_level == 'full':
            accepted_levels = ('full', )
        # both "full" and "differential" backups count as
        # "differential" backup
        elif backup_level == 'differential':
            accepted_levels = ('full', 'diff')
        # all backup levels count as "incremental" backup
        elif backup_level == 'incremental':
            accepted_levels = ('full', 'diff', 'incr')

        # loop over scheduled backups
        for index in range(len(scheduled_backups)):
            # we found the next scheduled backup when the current one
            # matches any of the accepted levels ...
            if scheduled_backups[index][1] in accepted_levels:
                next_backup = scheduled_backups[index][0]
                backup_level = scheduled_backups[index][1]

                # ... and it lies in the future
                if next_backup > now:
                    return (next_backup, backup_level)

        assert False, "this part of the code should never be reached!"


    def find_old_backups(self, prior_date=None):
        # prepare regex to filter valid backups
        regex_postfixes = '|'.join(self._postfixes.values())
        regex = re.compile('^({0})-({1})$'.format(self.date_regex,
                                                  regex_postfixes))

        # look for created backups (either real or simulated)
        found_backups = []

        # find all subdirectories in backup directory
        for dirname in os.listdir(self.backup_directory):
            # convert found path to absolute path
            full_path = os.path.join(self.backup_directory, dirname)

            # check whether found path is a directory ...
            if os.path.isdir(full_path):
                # ... which name matches the date/postfix regex
                m = regex.match(dirname)

                # extract path composition from matching paths
                if m is not None:
                    (timestamp, postfix) = m.groups()

                    # prepare search for backup catalog
                    catalog_name = '{0}-catalog.01.dar'.format(timestamp)
                    catalog_full = os.path.join(full_path, catalog_name)

                    # look for readable catalog file
                    if os.access(catalog_full, os.R_OK):
                        # regard path as valid backup
                        found_backups.append((timestamp, postfix))

        # sort found backups by path
        found_backups.sort(key=lambda i: str.lower(i[0]))

        # optinally filter found backups by date
        if isinstance(prior_date, datetime.datetime):
            found_backups_old = found_backups
            found_backups = []

            # loop over found backups
            for found_backup in found_backups_old:
                # convert timestamp to "datetime" object
                timestamp = found_backup[0]
                backup_date = datetime.datetime.strptime(
                    timestamp, self.date_format)

                # keep backups prior to (or at!) given date
                if backup_date <= prior_date:
                    found_backups.append(found_backup)

        # make result read-only
        found_backups = tuple(found_backups)

        #return result
        return found_backups


    @lalikan.utilities.Memoized
    def last_existing_backup(self, backup_level, now):
        self._check_backup_level(backup_level)

        # find existing backups
        found_backups = self.find_old_backups(now)

        # no backups were found
        if not found_backups:
            return None

        # only "full" backups count as "full" backup
        if backup_level == 'full':
            accepted_levels = ('full', )
        # both "full" and "differential" backups count as
        # "differential" backup
        elif backup_level == 'differential':
            accepted_levels = ('full', 'diff')
        # all backup levels count as "incremental" backup
        elif backup_level == 'incremental':
            accepted_levels = ('full', 'diff', 'incr')

        # backwards loop over found backups
        for n in range(len(found_backups), 0, -1):
            # sequences start at index zero
            index = n - 1

            # we found the last backup when the current one matches
            # any of the accepted levels
            if found_backups[index][1] in accepted_levels:
                last_existing = datetime.datetime.strptime(
                    found_backups[index][0], self.date_format)
                backup_level = found_backups[index][1]

                return (last_existing, backup_level)

        assert False, "this part of the code should never be reached!"


    @lalikan.utilities.Memoized
    def days_overdue(self, now, backup_level):
        last_scheduled = self.last_scheduled_backup(backup_level, now)
        last_existing = self.last_existing_backup(backup_level, now)

        # no scheduled backup lies in the past
        if last_scheduled is None:
            next_scheduled = self.next_scheduled_backup(backup_level, now)
            calculation_base = next_scheduled[0]
        # no backup of this level has been executed yet
        elif last_existing is None:
            calculation_base = last_scheduled[0]
        # last backup of this level is older than scheduled backup
        elif last_existing[0] < last_scheduled[0]:
            calculation_base = last_scheduled[0]
        # last executed backup is current
        else:
            next_scheduled = self.next_scheduled_backup(backup_level, now)
            calculation_base = next_scheduled[0]

        # calculate fractional days since/until scheduled backup
        days_overdue = ((now - calculation_base) / datetime.timedelta(days=1))

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
    def sanitise_path(self, path):
        # convert to absolute path
        path = os.path.abspath(path)

        # assert that path has a length
        assert len(path) > 0

        # Windows: DAR uses Cygwin internally
        if sys.platform == 'win32':
            # extract drive
            (drive, tail) = os.path.splitdrive(path)

            # lower space drive letter
            if drive:
                drive = drive[0].lower()

            # remove heading path separator from remaining path
            if tail.startswith(os.sep):
                tail = tail[len(os.sep):]

            # turn path to something like "/cygdrive/c/path/to/dar"
            path = os.path.join(os.sep, 'cygdrive', drive, tail)

            # Cygwin uses "/" as path separator
            path = path.replace(os.sep, '/')

        return path

    # ----- OLD CODE -----

    def get_backup_reference(self, backup_level):
        self._check_backup_level(backup_level)

        if backup_level == 'full':
            reference_base = 'none'
            reference_option = ''
        elif backup_level == 'differential':
            reference_base = self.name_of_last_backup('full')
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{0}-catalog'.format(reference_timestamp)

            full_path = os.path.join(self.backup_directory,
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)
        elif backup_level == 'incremental':
            last_full = self.days_since_last_backup('full')
            last_differential = self.days_since_last_backup('differential')
            last_incremental = self.days_since_last_backup('incremental')

            newest_backup = 'full'
            newest_age = last_full

            if (last_differential >= 0) and (last_differential < newest_age):
                newest_backup = 'differential'
                newest_age = last_differential

            if (last_incremental >= 0) and (last_incremental < newest_age):
                newest_backup = 'incremental'
                newest_age = last_incremental

            reference_base = self.name_of_last_backup(newest_backup)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{0}-catalog'.format(reference_timestamp)

            full_path = os.path.join(self.backup_directory,
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)

        return (reference_base, reference_option)
