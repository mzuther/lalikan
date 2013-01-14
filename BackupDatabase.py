# -*- coding: utf-8 -*-

"""Lalikan
   =======
   Backup scheduler for Disk ARchive (DAR)

   Copyright (c) 2010-2012 Martin Zuther (http://www.mzuther.de/)

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

from __future__ import print_function

import datetime
import gettext
import locale
import os
import re
import time

from Settings import *

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


class BackupDatabase:

    def __init__(self, section, settings, debugger):
        self.__section = section
        print('selected backup \'%s\'' % self.__section)

        if debugger:
            self.__backup_client = 'localhost'
        else:
            self.__backup_client = settings.get(section, 'backup_client', False)

        # port only required for client backup
        self.__backup_client_port = settings.get( \
            section, 'backup_client_port', self.__backup_client == 'localhost')

        self.__backup_directory = settings.get( \
            section, 'backup_directory', False)
        self.__backup_database = settings.get(section, 'backup_database', True)

        self.__backup_interval = { \
            'full': float(settings.get( \
                section, 'backup_interval_full', False)), \
            'differential': float(settings.get( \
                section, 'backup_interval_differential', False)), \
            'incremental': float(settings.get( \
                section, 'backup_interval_incremental', False))}

        self.__backup_options = settings.get(section, 'backup_options', True)
        self.__path_to_dar = settings.get(section, 'path_to_dar', True)
        self.__path_to_dar_manager = settings.get( \
             section, 'path_to_dar_manager', True)

        self.__date_format = settings.get(section, 'date_format', False)
        self.__date_regex = settings.get(section, 'date_regex', False)

        self.__backup_start_time = datetime.datetime.strptime( \
            settings.get(section, 'backup_start_time', False), \
                self.__date_format)

        self.__command_pre_run = settings.get(section, 'command_pre_run', True)
        self.__command_post_run = settings.get( \
            section, 'command_post_run', True)

        self.__backup_types = ('full', 'differential', 'incremental')
        self.__backup_postfixes = {'full': 'full', 'differential': 'diff', \
                                      'incremental': 'incr'}
        self.__last_backup_days = {'full': None, 'differential': None, \
                                       'incremental': None}
        self.__last_backup_file = {'full': None, 'differential': None, \
                                       'incremental': None}


    def get_backup_client(self):
        return self.__backup_client


    def get_backup_client_port(self):
        return self.__backup_client_port


    def get_path_to_dar(self):
        return self.__path_to_dar


    def get_path_to_dar_manager(self):
        return self.__path_to_dar_manager


    def get_backup_interval(self, backup_type):
        self.check_backup_type(backup_type)
        return self.__backup_interval[backup_type]


    def get_backup_directory(self):
        return self.__backup_directory


    def get_backup_postfix(self, backup_type):
        self.check_backup_type(backup_type)
        return self.__backup_postfixes[backup_type]


    def get_backup_options(self):
        return self.__backup_options


    def get_database(self):
        return self.__backup_database


    def get_date_format(self):
        return self.__date_format


    def get_pre_run_command(self):
        return self.__command_pre_run


    def get_post_run_command(self):
        return self.__command_post_run


    def check_backup_type(self, backup_type):
        if backup_type not in self.__backup_postfixes:
            raise ValueError('wrong backup type given ("%s")' % backup_type)


    def need_backup(self, force_backup, debugger):
        for backup_type in self.__backup_types:
            if self.__last_backup(backup_type, debugger) < 0:
                return backup_type

        if force_backup:
            return 'incremental (forced)'
        else:
            return 'none'


    def __last_backup(self, backup_type, debugger):
        self.check_backup_type(backup_type)

        backup_last = self.days_since_last_backup(backup_type, debugger)
        backup_due = self.__days_since_backup_due_date(backup_type, debugger)

        if backup_type == 'differential':
            backup_interval = self.__backup_interval['differential']
            backup_due_full = self.__days_since_backup_due_date( \
                'full', debugger)

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


    def find_old_backups(self, backup_type, prior_date, debugger):
        self.check_backup_type(backup_type)

        backup_postfix = self.__backup_postfixes[backup_type]
        regex = re.compile('^%s-%s$' % (self.__date_regex, backup_postfix))
        directories = []
        if debugger:
            for item in debugger['directories']:
                if regex.match(item):
                    directories.append(item)
        else:
            for item in os.listdir(self.__backup_directory):
                item_full = os.path.join(self.__backup_directory, item)
                # only add item if is a directory ...
                if os.path.isdir(item_full):
                    # ... which name matches the date/postfix regex ...
                    if regex.match(item):
                        timestamp = item.rsplit('-', 1)[0]
                        catalog_name = '%s-%s.01.dar' % (timestamp, "catalog")
                        catalog_full = os.path.join(self.__backup_directory, \
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
                item_date = datetime.datetime.strptime(item_date, \
                                                           self.__date_format)
                if item_date < prior_date:
                    directories.append(item)

        return directories


    def name_of_last_backup(self, backup_type, debugger):
        if self.__last_backup_file[backup_type] is not None:
            return self.__last_backup_file[backup_type]

        directories = self.find_old_backups(backup_type, None, debugger)
        if len(directories) == 0:
            return None
        else:
            self.__last_backup_file[backup_type] = directories[-1]
            return self.__last_backup_file[backup_type]


    def days_since_last_backup(self, backup_type, debugger):
        if self.__last_backup_days[backup_type] is not None:
            return self.__last_backup_days[backup_type]

        most_recent = self.name_of_last_backup(backup_type, debugger)

        if most_recent is None:
            return -1.0
        else:
            self.check_backup_type(backup_type)

            backup_postfix = self.__backup_postfixes[backup_type]
            most_recent = most_recent[:-len(backup_postfix) - 1]
            most_recent_datetime = datetime.datetime.strptime( \
                most_recent, self.__date_format)
            if debugger:
                now = debugger['now']
            else:
                now = datetime.datetime.utcnow()

            age = now - most_recent_datetime
            self.__last_backup_days[backup_type] = \
                age.days + age.seconds / 86400.0

            return self.__last_backup_days[backup_type]


    def __days_since_backup_due_date(self, backup_type, debugger):
        self.check_backup_type(backup_type)

        backup_postfix = self.__backup_postfixes[backup_type]
        if backup_type == 'full':
            if debugger:
                now = debugger['now']
            else:
                now = datetime.datetime.utcnow()

            time_passed = now - self.__backup_start_time
            days_passed = time_passed.days + time_passed.seconds / 86400.0
        else:
            days_passed = self.__days_since_backup_due_date('full', debugger)

        # backup start date lies in the future
        if days_passed < 0:
            return days_passed

        days_since_due_date = days_passed % self.__backup_interval[backup_type]
        return days_since_due_date


    def days_to_next_backup_due_date(self, backup_type, debugger):
        self.check_backup_type(backup_type)

        remaining_days = -self.__days_since_backup_due_date( \
            backup_type, debugger)

        if remaining_days < 0:
            remaining_days += self.__backup_interval[backup_type]

        return remaining_days
