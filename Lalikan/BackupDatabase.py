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

# --- ALREADY CONVERTED ---

import datetime
import gettext
import os
import re

import Settings

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


class BackupDatabase:
    def __init__(self, section, settings):
        def get_setting(setting, allow_empty):
            return settings.get(section, setting, allow_empty)

        self._debugger = None

        self._section = section
        print('selected backup "{0}"'.format(self._section))

        self._backup_client = get_setting('backup_client', False)

        # for local backups, a port number is not required
        port_not_required = (self._backup_client == 'localhost')
        self._backup_client_port = get_setting('backup_client_port',
                                               port_not_required)

        self._backup_directory = get_setting('backup_directory', False)
        self._backup_database = get_setting('backup_database', True)

        self._backup_interval = {}
        self._backup_postfixes = {}
        self._last_backup_days = {}
        self._last_backup_file = {}

        self._backup_types = ('full', 'differential', 'incremental')
        for backup_type in self._backup_types:
            self._backup_interval[backup_type] = float(get_setting(
                'backup_interval_{0}'.format(backup_type), False))

            self._backup_postfixes[backup_type] = backup_type[:4]
            self._last_backup_days[backup_type] = None
            self._last_backup_file[backup_type] = None

        self._backup_options = get_setting('backup_options', True)
        self._path_to_dar = get_setting('path_to_dar', True)
        self._path_to_dar_manager = get_setting('path_to_dar_manager', True)

        self._date_format = get_setting('date_format', False)
        self._date_regex = get_setting('date_regex', False)

        self._backup_start_time = datetime.datetime.strptime(
            get_setting('backup_start_time', False), self._date_format)

        self._command_pre_run = get_setting('command_pre_run', True)
        self._command_post_run = get_setting('command_post_run', True)


    def _check_backup_type(self, backup_type):
        if backup_type not in self._backup_postfixes:
            raise ValueError(
                'wrong backup type given ("{0}")'.format(backup_type))


    def get_backup_client(self):
        return self._backup_client


    def get_backup_client_port(self):
        return self._backup_client_port


    def get_path_to_dar(self):
        return self._path_to_dar


    def get_path_to_dar_manager(self):
        return self._path_to_dar_manager


    def get_backup_interval(self, backup_type):
        self._check_backup_type(backup_type)
        return self._backup_interval[backup_type]


    def get_backup_directory(self):
        return self._backup_directory


    def get_backup_postfix(self, backup_type):
        self._check_backup_type(backup_type)
        return self._backup_postfixes[backup_type]


    def get_backup_options(self):
        return self._backup_options


    def get_database(self):
        return self._backup_database


    def get_date_format(self):
        return self._date_format


    def get_pre_run_command(self):
        return self._command_pre_run


    def get_post_run_command(self):
        return self._command_post_run


    def need_backup(self, force_backup):
        for backup_type in self._backup_types:
            if self._last_backup(backup_type) < 0:
                return backup_type

        # no backup scheduled
        if force_backup:
            return 'incremental (forced)'
        else:
            return 'none'

    # --- NOT YET CONVERTED ---

    def _last_backup(self, backup_type):
        self._check_backup_type(backup_type)

        backup_last = self.days_since_last_backup(backup_type)
        backup_due = self._days_since_backup_due_date(backup_type)

        if backup_type == 'differential':
            backup_interval = self._backup_interval['differential']
            backup_due_full = self._days_since_backup_due_date('full')

            # differential backups should pause for one backup
            # interval after a full backup is due
            if (backup_last < 0) or (backup_due_full < backup_interval):
                backup_due = backup_due_full - backup_interval

        # skip backup
        if backup_due < 0:
            return 1
        # no previous backup found, so mark it as due
        elif backup_last < 0:
            return -1
        # mark backup as due when last backup is older than its
        # scheduled date
        else:
            return backup_due - backup_last


    def find_old_backups(self, backup_type, prior_date):
        self._check_backup_type(backup_type)

        backup_postfix = self._backup_postfixes[backup_type]
        regex = re.compile('^{0}-{1}$'.format(self._date_regex, backup_postfix))
        directories = []
        if self._debugger:
            for item in self._debugger['directories']:
                if regex.match(item):
                    directories.append(item)
        else:
            for item in os.listdir(self._backup_directory):
                item_full = os.path.join(self._backup_directory, item)
                # only add item if is a directory ...
                if os.path.isdir(item_full):
                    # ... which name matches the date/postfix regex ...
                    if regex.match(item):
                        timestamp = item.rsplit('-', 1)[0]
                        catalog_name = '{0}-catalog.01.dar'.format(timestamp)
                        catalog_full = os.path.join(self._backup_directory,
                                                    item, catalog_name)
                        # ... and which contains a readable catalog file
                        if os.access(catalog_full, os.R_OK):
                            directories.append(item)

        directories.sort()

        if type(prior_date) == datetime.datetime:
            old_directories = directories
            directories = []

            for item in old_directories:
                item_date = item[:-len(backup_postfix) - 1]
                item_date = datetime.datetime.strptime(
                    item_date, self._date_format)
                if item_date < prior_date:
                    directories.append(item)

        return directories


    def name_of_last_backup(self, backup_type):
        if self._last_backup_file[backup_type] is not None:
            return self._last_backup_file[backup_type]

        directories = self.find_old_backups(backup_type, None)
        if len(directories) == 0:
            return None
        else:
            self._last_backup_file[backup_type] = directories[-1]
            return self._last_backup_file[backup_type]


    def days_since_last_backup(self, backup_type):
        if self._last_backup_days[backup_type] is not None:
            return self._last_backup_days[backup_type]

        most_recent = self.name_of_last_backup(backup_type)

        if most_recent is None:
            return -1.0
        else:
            self._check_backup_type(backup_type)

            backup_postfix = self._backup_postfixes[backup_type]
            most_recent = most_recent[:-len(backup_postfix) - 1]
            most_recent_datetime = datetime.datetime.strptime(
                most_recent, self._date_format)
            if self._debugger:
                now = self._debugger['now']
            else:
                now = datetime.datetime.now()

            age = now - most_recent_datetime
            self._last_backup_days[backup_type] = (age.days +
                                                   age.seconds / 86400.0)

            return self._last_backup_days[backup_type]


    def _days_since_backup_due_date(self, backup_type):
        self._check_backup_type(backup_type)

        backup_postfix = self._backup_postfixes[backup_type]
        if backup_type == 'full':
            if self._debugger:
                now = self._debugger['now']
            else:
                now = datetime.datetime.now()

            time_passed = now - self._backup_start_time
            days_passed = time_passed.days + time_passed.seconds / 86400.0
        else:
            days_passed = self._days_since_backup_due_date('full')

        # backup start date lies in the future
        if days_passed < 0:
            return days_passed

        days_since_due_date = days_passed % self._backup_interval[backup_type]
        return days_since_due_date


    def days_to_next_backup_due_date(self, backup_type):
        self._check_backup_type(backup_type)

        remaining_days = -self._days_since_backup_due_date(backup_type)

        if remaining_days < 0:
            remaining_days += self._backup_interval[backup_type]

        return remaining_days

    # --- ALREADY CONVERTED ---

    def sanitise_path(self, path):
        path = os.path.abspath(path)

        if (os.name == 'nt') and (len(path) > 0):
            (drive, tail) = os.path.splitdrive(path)

            if drive:
                drive = drive[0].lower()

            if tail.startswith(os.sep):
                tail = tail[len(os.sep):]

            path = os.path.join(os.sep, drive, 'cygdrive', tail)
            path = path.replace('\\', '/')

        return path


    def get_backup_reference(self, backup_type):
        self._check_backup_type(backup_type)

        if backup_type == 'full':
            reference_base = 'none'
            reference_option = ''
        elif backup_type == 'differential':
            reference_base = self.name_of_last_backup('full')
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '{0}-catalog'.format(reference_timestamp)

            full_path = os.path.join(self.get_backup_directory(),
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)
        elif backup_type == 'incremental':
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

            full_path = os.path.join(self.get_backup_directory(),
                                     reference_base, reference_catalog)
            reference_option = '--ref ' + self.sanitise_path(full_path)

        return (reference_base, reference_option)


    def test(self, number_of_days, backup_interval, start_datetime):
        self._debugger = {}
        self._debugger['directories'] = []
        self._debugger['references'] = []
        self._debugger['now'] = start_datetime

        number_of_backups = int(number_of_days / backup_interval) + 2
        current_days = [(backup_id * backup_interval)
                        for backup_id in range(number_of_backups)
                        if (backup_id * backup_interval) <= number_of_days]

        for current_day in current_days:
            created_backup = self.test_run()

            if not created_backup:
                print()
            print('----- {0} -----'.format(
                    self._debugger['now'].strftime(self.get_date_format())))

            if created_backup:
                for n in range(len(self._debugger['directories'])):
                    directory = self._debugger['directories'][n]
                    reference = self._debugger['references'][n]

                    if reference.endswith(self.get_backup_postfix(
                            'differential')):
                        reference = '    {reference}'.format(**locals())
                    elif reference.endswith(self.get_backup_postfix(
                            'incremental')):
                        reference = '        {reference}'.format(**locals())

                    if directory.endswith(self.get_backup_postfix('full')):
                        print('{directory}            {reference}'.format(
                                **locals()))
                    elif directory.endswith(self.get_backup_postfix(
                            'differential')):
                        print('    {directory}        {reference}'.format(
                                **locals()))
                    else:
                        print('        {directory}    {reference}'.format(
                                **locals()))
                print('----------------------------\n')

            self._debugger['now'] += datetime.timedelta(backup_interval)


    def test_run(self):
        backup_type = self.need_backup(False)

        print('\nnext full in  {0:7.3f} days  ({1:7.3f})'.format(
                self.days_to_next_backup_due_date('full'),
                self.get_backup_interval('full')))
        print('next diff in  {0:7.3f} days  ({1:7.3f})'.format(
                self.days_to_next_backup_due_date('differential'),
                self.get_backup_interval('differential')))
        print('next incr in  {0:7.3f} days  ({1:7.3f})\n'.format(
                self.days_to_next_backup_due_date('incremental'),
                self.get_backup_interval('incremental')))

        print('backup type:  {0}\n'.format(backup_type))

        if backup_type == 'none':
            return False
        else:
            backup_postfix = self.get_backup_postfix(backup_type)
            (reference_base, reference_option) = self.get_backup_reference(
                backup_type)

            print()
            now = self._debugger['now']
            timestamp = now.strftime(self.get_date_format())

            base_name = '{timestamp}-{backup_postfix}'.format(**locals())
            catalog_name = '{0}-catalog'.format(timestamp)

            self._debugger['directories'].append(base_name)
            self._debugger['references'].append(reference_base)

            self.__delete_old_backups(backup_type)
            return True

    # --- NOT YET CONVERTED ---

    def __delete_old_backups(self, backup_type):
        def delete_backup(basename):
            print('deleting old backup "{0}"'.format(basename))
            n = self._debugger['directories'].index(basename)

            del self._debugger['directories'][n]
            del self._debugger['references'][n]


        backup_postfix = self.get_backup_postfix(backup_type)
        remove_prior = self.find_old_backups(backup_type, None)
        if len (remove_prior) < 2:
            return
        else:
            # get date of previous backup of same type
            prior_date = remove_prior[-2]
            prior_date = prior_date[:-len(backup_postfix) - 1]
            prior_date = datetime.datetime.strptime(
                prior_date, self.get_date_format())

        # please remember: never delete full backups!
        if backup_type == 'full':
            print('\nfull: removing diff and incr prior to last full (%s)\n' % \
                prior_date)

            for basename in self.find_old_backups('incremental', prior_date):
                delete_backup(basename)
            for basename in self.find_old_backups('differential', prior_date):
                delete_backup(basename)

            # separate check for old differential backups
            backup_postfix_diff = self.get_backup_postfix('differential')
            remove_prior_diff = self.find_old_backups('differential', None)

            if (len (remove_prior) > 1) and (len(remove_prior_diff) > 0):
                # get date of last full backup
                last_full_date = remove_prior[-1]
                last_full_date = last_full_date[:-len(backup_postfix) - 1]
                last_full_date = datetime.datetime.strptime(
                    last_full_date, self.get_date_format())

                # get date of last differential backup
                last_diff_date = remove_prior_diff[-1]
                last_diff_date = last_diff_date[:-len(backup_postfix_diff) - 1]
                last_diff_date = datetime.datetime.strptime(
                    last_diff_date, self.get_date_format())

                print('\nfull: removing incr prior to last diff (%s)\n' % \
                    last_diff_date)

                for basename in self.find_old_backups('incremental',
                                                      last_diff_date):
                    delete_backup(basename)

        elif backup_type == 'differential':
            print('\ndiff: removing incr prior to last diff (%s)\n' % \
                prior_date)

            for basename in self.find_old_backups('incremental', prior_date):
                delete_backup(basename)
        elif backup_type == 'incremental':
            return

# --- ALREADY CONVERTED ---

if __name__ == '__main__':
    section = 'Default'
    number_of_days = 60
    interval = 4.0
    start_time = datetime.datetime.now()

    print()
    settings = Settings.Settings('/etc/lalikan')
    bd = BackupDatabase(section, settings)

    bd.test(number_of_days, interval, start_time)
    print()
