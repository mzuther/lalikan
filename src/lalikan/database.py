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
import functools
import gettext
import os
import re
import sys

import lalikan.properties

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
        # 0: full backup, contains everything
        # 1: differential, contains all changes since last full backup
        # 2: incremental, contains all changes since last backup
        self._backup_levels = (0, 1, 2)

        # backup file name postfixes time for all backup levels
        self._postfixes = ('full', 'diff', 'incr')

        # set point in time to current date and time
        self.point_in_time = datetime.datetime.now()


    def clear_cache(self):
        """
        Clear cached function return values.

        """
        self.__memoized = {}


    def memoize_function(function):
        """
        Decorator for memoizing function return values.

        **Caution: may only be used with class methods (automatically
        binds first parameter to "self").  Also, "kwargs" parameters
        are ignored.**

        """
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            # caching of **kwargs arguments is not supported
            if len(kwargs) > 0:
                raise NotImplementedError('decorator @memoize_function does '
                                          'not support **kwargs')

            # bind first parameter to "self"
            self = args[0]

            # create dictionary key from function name
            key = function.__name__

            # add string representations function arguments to key
            # (skip "self" though)
            for arg in args[1:]:
                key += '_' + repr(arg)

            # try to return the cached function return value
            try:
                return self.__memoized[key]
            # calculate and cache return value
            except KeyError:
                self.__memoized[key] = function(*args, **kwargs)
                return self.__memoized[key]

        # return decorated function
        return wrapper


    @property
    def point_in_time(self):
        """
        Attribute (read/write): point in time to which all backup
        operations relate.  When an instance of this class is created,
        this is set to the current date and time.

        :returns:
            point in time
        :rtype:
            :py:mod:`datetime.datetime`

        """
        return self._point_in_time


    @point_in_time.setter
    def point_in_time(self, time_point):
        # update point in time
        self._point_in_time = time_point

        # clear cached function return values
        self.clear_cache()


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
            :py:mod:`datetime.timedelta`

        """
        interval = float(self._get_option('interval_full'))
        return datetime.timedelta(interval)


    @property
    def interval_diff(self):
        """
        Attribute: interval for "differential" backups.

        :returns:
            backup interval in days
        :rtype:
            :py:mod:`datetime.timedelta`

        """
        interval = float(self._get_option('interval_diff'))
        return datetime.timedelta(interval)


    @property
    def interval_incr(self):
        """
        Attribute: interval for "incremental" backups.

        :returns:
            backup interval in days
        :rtype:
            :py:mod:`datetime.timedelta`

        """
        interval = float(self._get_option('interval_incr'))
        return datetime.timedelta(interval)


    @property
    def postfix_full(self):
        """
        Attribute: file postfix for "full" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes[0]


    @property
    def postfix_diff(self):
        """
        Attribute: file postfix for "diff" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes[1]


    @property
    def postfix_incr(self):
        """
        Attribute: file postfix for "incr" backups.

        :returns:
            backup file postfix
        :rtype:
            String

        """
        return self._postfixes[2]


    @property
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
            backup level (0 to 2)
        :type backup_level:
            integer

        :returns:
            accepted backup levels
        :rtype:
            list

        """
        # count all backup levels above given backup level as valid
        return self._backup_levels[0:backup_level + 1]


    def _check_backup_level(self, backup_level):
        """
        Checks whether the specified backup level (such as "full")
        is allowed.

        :param backup_level:
            backup level
        :type backup_level:
            integer

        :raises:
            :py:class:`ValueError`
        :rtype:
            None

        """
        if backup_level not in self._backup_levels:
            raise ValueError(
                'wrong backup level given ("{0}")'.format(backup_level))


    def _fill_schedule(self, schedule, backup_level):
        """
        Fill schedule with backups of a certain backup level using a given
        interval.

        :param schedule:
            existing schedule containing times and corresponding
            backup levels
        :type schedule:
            list of lalikan.properties.BackupProperties

        :param backup_level:
            backup level (1 or 2)
        :type backup_level:
            integer

        :returns:
            updated schedule containing times and corresponding backup
            levels
        :rtype:
            list of lalikan.properties.BackupProperties

        """
        # check backup level
        if backup_level == 1:
            interval = self.interval_diff
        elif backup_level == 2:
            interval = self.interval_incr
        else:
            raise ValueError(
                'wrong backup level given ("{0}")'.format(backup_level))

        # temporary storage for newly scheduled backups
        temp_schedule = []

        # loop over existing schedule, ignoring the last entry
        for n in range(len(schedule) - 1):
            # start with current scheduled backup
            start_time = schedule[n].date

            # end with next scheduled backup
            end_time = schedule[n + 1].date

            # add one interval so that existing and new backup do not
            # overlap
            start_time += interval

            # calculate backup start times
            while start_time < end_time:
                # store new backup
                new_backup = lalikan.properties.BackupProperties(
                    start_time, backup_level)
                temp_schedule.append(new_backup)

                # move on
                start_time += interval

        # consolidate backup start times
        schedule.extend(temp_schedule)

        # sort consolidated schedule by date and time
        schedule = sorted(schedule)

        # return updated schedule
        return schedule


    @memoize_function
    def calculate_backup_schedule(self):
        """
        Calculate backup schedule, starting from the "full" backup prior to
        self.point_in_time and ending with the next one.

        :returns:
            backup schedule
        :rtype:
            list of lalikan.properties.BackupProperties

        """
        # calculate first "full" backup after the given date
        current_start_time = self.start_time
        while current_start_time <= self.point_in_time:
            current_start_time += self.interval_full

        # store upcoming "full" backup
        new_backup = lalikan.properties.BackupProperties(
            current_start_time, 0)
        schedule = [new_backup]

        # calculate previous "full" backup
        current_start_time -= self.interval_full

        # store previous "full" backup (if valid)
        if current_start_time >= self.start_time:
            new_backup = lalikan.properties.BackupProperties(
                current_start_time, 0)

            # store at the beginning of the list
            schedule.insert(0, new_backup)

        # found "full" backup prior to given date
        if len(schedule) > 1:
            # fill schedule with "diff" backups
            schedule = self._fill_schedule(schedule, 1)

            # fill schedule with "incr" backups
            schedule = self._fill_schedule(schedule, 2)

        # return schedule
        return schedule


    def _current_scheduled_backup(self, backup_level):
        """
        Find current scheduled backup for a given backup level.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            integer

        :returns:
            current scheduled backup
        :rtype:
            lalikan.properties.BackupProperties

        """
        # find scheduled backups
        schedule = self.calculate_backup_schedule()

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # backwards loop over schedule
        for backup in reversed(schedule):
            # we found the current scheduled backup when it matches
            # any of the accepted levels ...
            if backup.level in accepted_levels:
                # ... and it doesn't lie in the future
                if backup.date <= self.point_in_time:
                    return backup

        # no matching scheduled backup found
        return lalikan.properties.BackupProperties(None, backup_level)


    def find_existing_backups(self, prior_to=None):
        """
        Find existing backups.  The result can be filtered to backups
        prior to (or exactly at) a given point in time.

        :param prior_to:
            given point in time
        :type prior_to:
            :py:mod:`datetime.datetime` or None

        :returns:
            list of existing backups
        :rtype:
            list of lalikan.properties.BackupProperties

        """
        # get subdirectories in backup directory
        subdirectories = [f for f in os.listdir(self.backup_directory)
                       if os.path.isdir(os.path.join(self.backup_directory, f))]

        # look for existing backups
        existing_backups = []

        # loop over subdirectories
        for dirname in subdirectories:
            # check whether the path matches the regular expression
            # for backup directory names
            match = self.backup_regex.match(dirname)

            # path name matches regular expression
            if match:
                # extract path elements
                timestamp, suffix = match.groups()

                # convert timestamp to "datetime" object
                date = datetime.datetime.strptime(timestamp, self.date_format)

                # convert suffix to backup level
                backup_level = self._postfixes.index(suffix)

                # convert to absolute path name
                full_path = os.path.join(self.backup_directory, dirname)

                # valid backups contain a backup catalog; prepare a
                # search for this catalog
                catalog_name = '{0}-catalog.01.dar'.format(timestamp)
                catalog_path = os.path.join(full_path, catalog_name)

                # catalog file exists
                if os.path.isfile(catalog_path):
                    # regard path as valid backup
                    existing_backups.append(
                        lalikan.properties.BackupProperties(
                            date, backup_level))

        # sort backups by date and time
        existing_backups.sort()

        # optionally filter backups by date
        if prior_to:
            temp = []

            # loop over backups
            for backup in existing_backups:
                # keep only backups prior to (or exactly at) given date
                if backup.date <= prior_to:
                    temp.append(backup)

            # store filtered backups
            existing_backups = temp

        # return result
        return existing_backups


    def last_existing_backup(self, backup_level):
        """
        Find last existing backup for a given backup level.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            index

        :returns:
            last existing backup
        :rtype:
            lalikan.properties.BackupProperties

        """
        # find existing backups prior to (or exactly at) given date
        existing_backups = self.find_existing_backups(self.point_in_time)

        # no backups were found
        if len(existing_backups) == 0:
            return lalikan.properties.BackupProperties(None, backup_level)

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # backwards loop over existing backups
        for backup in reversed(existing_backups):
            # we found the last backup when the current one matches
            # any of the accepted levels
            if backup.level in accepted_levels:
                return backup

        # no matching existing backup found
        return lalikan.properties.BackupProperties(None, backup_level)


    def last_scheduled_backup(self, backup_level):
        """
        Find last scheduled backup for a given backup level.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            index

        :returns:
            last scheduled backup
        :rtype:
            lalikan.properties.BackupProperties

        """
        # assert valid backup level
        self._check_backup_level(backup_level)

        # get date of last existing backup of given (or lower) level
        last_existing = self.last_existing_backup(backup_level)
        last_existing_date = last_existing.date

        # if this fails, use date of the Epoch so that comparisons
        # with "datetime" objects leave meaningful results
        if not last_existing.is_valid:
            last_existing_date = datetime.datetime(1970, 1, 1)

        # loop over current and previous backup levels
        for test_level in range(backup_level + 1):
            # do we need a backup of the tested backup level?
            backup_needed = self._current_scheduled_backup(test_level)

            # return result for current backup level
            if test_level == backup_level:
                return backup_needed
            # return result if a backup is needed (otherwise, keep
            # running...)
            elif backup_needed.is_valid and \
                    backup_needed.date > last_existing_date:
                return backup_needed


    @memoize_function
    def next_scheduled_backup(self, backup_level):
        """
        Find next upcoming scheduled backup for a given backup level.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            index

        :returns:
            next scheduled backup
        :rtype:
            lalikan.properties.BackupProperties

        """
        # assert valid backup level
        self._check_backup_level(backup_level)

        # get backup levels that will be accepted as substitute for
        # given backup level
        accepted_levels = self._accepted_backup_levels(backup_level)

        # find scheduled backups
        scheduled_backups = self.calculate_backup_schedule()

        # if the code functions as expected, there will always be one
        # or more scheduled backups
        assert scheduled_backups, 'no backups scheduled.  Sorry, this ' \
            'should not have happened!'

        # loop over scheduled backups
        for scheduled_backup in scheduled_backups:
            # we found the next scheduled backup when the current one
            # matches any of the accepted levels and lies in the future
            if scheduled_backup.level in accepted_levels and \
                    scheduled_backup.date > self.point_in_time:
                return scheduled_backup

        assert False, 'this part of the code should never be reached!'


    def days_overdue(self, backup_level):
        """
        Calculate number of days that a backup is due.

        :param backup_level:
            backup level (0 to 2)
        :type backup_level:
            index

        :returns:
            days that have passed since a backup has become due
            (positive numbers; backup is necessary) or that are left
            until a backup will be due (negative numbers)
        :rtype:
            float

        """
        # calculate last scheduled backup
        last_scheduled = self.last_scheduled_backup(backup_level)

        # find last existing backup
        last_existing = self.last_existing_backup(backup_level)

        # calculate next upcoming scheduled backup
        next_scheduled = self.next_scheduled_backup(backup_level)

        # no scheduled backup lies in the past, so calculate time to
        # the next upcoming one
        if not last_scheduled.is_valid:
            scheduled_backup = next_scheduled
        # no backup of this level has been executed yet, so calculate
        # time from the last existing backup
        elif not last_existing.is_valid:
            scheduled_backup = last_scheduled
        # last backup of this level is older than scheduled backup, so
        # calculate time from the last existing backup
        elif last_existing.date < last_scheduled.date:
            scheduled_backup = last_scheduled
        # last executed backup is current, so calculate time to the
        # next upcoming one
        else:
            scheduled_backup = next_scheduled

        # calculate days since/until scheduled backup (instance of
        # datetime.timedelta)
        timedelta_overdue = self.point_in_time - scheduled_backup.date

        # convert datetime.timedelta to fractional days
        days_overdue = timedelta_overdue / datetime.timedelta(days=1)

        return days_overdue


    def backup_needed(self, force_backup):
        """
        Find out whether a backup is necessary for self.point_in_time.

        :param force_backup:
            **True** forces a backup
        :type force_backup:
            Boolean

        :returns:
            backup level when backup is needed ("-1" for forced backups)
            and **None** otherwise
        :rtype:
            integer or None

        """
        # check necessity of a backup for all backup levels
        for level in self._backup_levels:
            # a backup of this level is necessary
            if self.days_overdue(level) >= 0.0:
                return level

        # force backup, but only after schedule begins
        if force_backup and self.point_in_time >= self.start_time:
            return -1

        # no backup necessary
        return None


    def sanitise_path(self, path_name):
        """
        Return a normalised absolutised version of the specified path name.
        On Windows, the path name will also be converted to something that
        Cygwin understands (such as "/cygdrive/c/path/to/dar").

        :param path_name:
            path name
        :type path_name:
            String

        :raises:
            :py:class:`ValueError`
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
