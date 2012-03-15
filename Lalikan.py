#!/usr/bin/env python
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

import datetime
import gettext
import glob
import locale
import os
import re
import socket
import subprocess
import time
import types

from optparse import OptionParser
from Settings import *


# set standard localisation for application
locale.setlocale(locale.LC_ALL, '')

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


class Lalikan:
    _INFORMATION = 'information'
    _WARNING = 'warning'
    _ERROR = 'error'
    _FINAL_ERROR = 'error'

    def __init__(self):
        # initialise version information, ...
        version_long = _('%(description)s\n%(copyrights)s\n\n%(license)s') % \
            {'description':settings.get_description(True), \
                 'copyrights':settings.get_copyrights(), \
                 'license':settings.get_license(True)}
        # ... usage information and ...
        usage = 'Usage: %(cmd_line)s [options]' % \
            {'cmd_line':settings.get_variable('cmd_line')}
        # ... the command line parser itself
        parser = OptionParser(usage=usage, version=version_long)

        # add command line options
        parser.add_option('-l', '--list', \
                              action='store_true', \
                              dest='list_sections', \
                              default=False, \
                              help=_('list all sections defined in the configuration file'))
        parser.add_option('-s', '--section', \
                              action='store', \
                              dest='section', \
                              metavar='SECTION', \
                              default=None, \
                              help=_('run SECTION from configuration file'))
        parser.add_option('--force', \
                              action='store_true', \
                              dest='force_backup', \
                              default=False, \
                              help=_('force backup'))

        # parse command line
        (options, args) = parser.parse_args()

        print settings.get_description(False)

        self.__section = options.section
        self.__force_backup = options.force_backup

        if options.list_sections:
            print 'The following sections have been defined in the configuration file:\n'

            for section in settings.sections():
                print ' * %s' % section

            print
            exit(0)

        if (self.__section not in settings.sections()) and \
                (type(self.__section) != types.NoneType):
            print 'The specified section \'%s\' has not been defined.  Please use one of these:\n' % \
                options.section

            for section in settings.sections():
                print ' * %s' % section

            print
            exit(1)


    def __initialise(self, section):
        print 'selected backup \'%s\'' % section

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


    def test(self, number_of_days, interval=1.0):
        current_day = 0.0
        debugger = {'directories': [], 'references': [], \
                        'now': datetime.datetime.utcnow()}

        while current_day <= number_of_days:
            if current_day > 0.0:
                debugger['now'] += datetime.timedelta(interval)
            current_day += interval

            created_backup = self.run(debugger)

            if not created_backup:
                print
            print '----- %s ------' % debugger['now'].strftime( \
                self.__date_format)
            if created_backup:
                for n in range(len(debugger['directories'])):
                    directory = debugger['directories'][n]
                    reference = debugger['references'][n]

                    if reference.endswith( \
                            self.__backup_postfixes['differential']):
                        reference = '    %s' % reference
                    elif reference.endswith( \
                            self.__backup_postfixes['incremental']):
                        reference = '        %s' % reference

                    if directory.endswith(self.__backup_postfixes['full']):
                        print '%s            %s' % (directory, reference)
                    elif directory.endswith( \
                            self.__backup_postfixes['differential']):
                        print '    %s        %s' % (directory, reference)
                    else:
                        print '        %s    %s' % (directory, reference)
                print '----------------------------\n'


    def run(self, debugger=None):
        if debugger:
            try:
                if type(self.__section) == types.NoneType:
                    self.__run(settings.sections()[0], debugger)
                else:
                    self.__run(self.__section, debugger)
            except OSError, e:
                print e
                exit(1)
        elif type(self.__section) == types.NoneType:
            for self.__section in settings.sections():
                error = False

                try:
                    self.__run(self.__section, debugger)
                except OSError, e:
                    error = True
                    print e

                    if error:
                        self._notify_user('At least one error has occurred!', \
                                              self._FINAL_ERROR, debugger)
        elif self.__section in settings.sections():
            try:
                self.__run(self.__section, debugger)
            except OSError, e:
                print e
                exit(1)
        else:
            exit(1)


    def __run(self, section, debugger=None):
        self.__initialise(section)

        if not debugger and os.name == 'posix':
            # check whether the script runs with superuser rights
            if (os.getuid() != 0) or (os.getgid() != 0):
                print '\n%s\n' % \
                    _('You have to run this application with superuser rights.')
                exit(1)

        if not self.__client_is_online():
            return False

        if debugger:
            self.__backup_client = 'localhost'
        else:
            self.__pre_run(section, debugger)

        need_backup = 'none'
        try:
            need_backup = self.__need_backup(debugger)

            print '\nnext full in  %7.3f days  (%7.3f)' % \
                (self.__days_to_next_backup_due_date('full', debugger), \
                     self.__backup_interval['full'])
            print 'next diff in  %7.3f days  (%7.3f)' % \
                (self.__days_to_next_backup_due_date( \
                    'differential', debugger), \
                     self.__backup_interval['differential'])
            print 'next incr in  %7.3f days  (%7.3f)\n' % \
                (self.__days_to_next_backup_due_date('incremental', debugger), \
                     self.__backup_interval['incremental'])

            print 'backup type:  %s\n' % need_backup

            if need_backup == 'none':
                return False
            elif need_backup == 'incremental (forced)':
                need_backup = 'incremental'

            self._notify_user('Starting %s backup...' % need_backup, \
                                  self._INFORMATION, debugger)

            self.__create_backup(need_backup, debugger)
        finally:
            if not debugger:
                self.__post_run(section, need_backup, debugger)

        return True


    def __client_is_online(self):
        # running DAR on localhost -- should be online ... :)
        if self.__backup_client == 'localhost':
            return True

        # check port availability up to three times
        for n in range(3):
            # wait 10 seconds between checks
            if n > 0:
                time.sleep(10.0)

            # initialise socket and time-out
            port = socket.socket()
            port.settimeout(2.0)

            try:
                # check port
                port.connect((self.__backup_client, \
                                  int(self.__backup_client_port)))

                # client is online
                return True
            except Exception, e:
                # an error occurred
                print e

        # no connection to client possible
        print 'Host "%(host)s" does not listen on port %(port)s.' % \
            {'host': self.__backup_client, 'port': self.__backup_client_port}
        return False


    def __remove_empty_directories__(self):
        """Remove empty directories from backup filetree

        Keyword arguments:
        None

        Return value:
        None

        """
        # initialise loop variable
        repeat = True

        while repeat:
            # will break loop until updated
            repeat = False

            # find empty directories
            for root, directories, files in \
                    os.walk(self.__backup_directory, topdown=False):
               # do not remove root directory
                if root != self.__backup_directory:
                    # directory is empty
                    if (len(directories) < 1) and (len(files) < 1):
                        # keep looping to find *all* empty directories
                        repeat = True
                        print _('removing empty directory "%(directory)s"') % \
                            {'directory':root}
                        # delete empty directory
                        os.rmdir(root)


    def __pre_run(self, section, debugger=None):
        if self.__command_pre_run:
            print 'pre-run command: %s' % self.__command_pre_run
            # stdin is needed to be able to communicate with the
            # application (i.e. answer a question)
            proc = subprocess.Popen( \
                self.__command_pre_run, shell=True, stdin=subprocess.PIPE, \
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                self._notify_user(output[0], self._INFORMATION, debugger)
            if output[1]:
                self._notify_user(output[1], self._ERROR, debugger)

        # recursively create root directory if it doesn't exist
        if not os.path.exists(self.__backup_directory):
            os.makedirs(self.__backup_directory)

        self.__remove_empty_directories__()


    def __post_run(self, section, need_backup, debugger=None):
        if self.__command_post_run:
            print 'post-run command: %s' % self.__command_post_run
            proc = subprocess.Popen( \
                self.__command_post_run, shell=True, stdin=subprocess.PIPE, \
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                self._notify_user(output[0], self._INFORMATION, debugger)
            if output[1]:
                self._notify_user(output[1], self._ERROR, debugger)

        if need_backup != 'none':
            self._notify_user('Finished backup.', self._INFORMATION, debugger)

        print '---'


    def __create_backup(self, backup_type, debugger):
        print
        if backup_type not in self.__backup_postfixes:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        backup_postfix = self.__backup_postfixes[backup_type]

        if backup_type == 'full':
            reference_base = 'none'
            reference_option = ''
        elif backup_type == 'differential':
            reference_base = self.__name_of_last_backup('full', debugger)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '%s-%s' % (reference_timestamp, "catalog")
            reference_option = '--ref ' + self._sanitise_path(os.path.join( \
                self.__backup_directory, reference_base, reference_catalog))
        elif backup_type == 'incremental':
            last_full = self.__days_since_last_backup( \
                'full', debugger)
            last_differential = self.__days_since_last_backup( \
                'differential', debugger)
            last_incremental = self.__days_since_last_backup( \
                'incremental', debugger)

            newest_backup = 'full'
            newest_age = last_full

            if (last_differential >= 0) and (last_differential < newest_age):
                newest_backup = 'differential'
                newest_age = last_differential

            if (last_incremental >= 0) and (last_incremental < newest_age):
                newest_backup = 'incremental'
                newest_age = last_incremental

            reference_base = self.__name_of_last_backup(newest_backup, debugger)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '%s-%s' % (reference_timestamp, "catalog")
            reference_option = '--ref ' + self._sanitise_path(os.path.join( \
                self.__backup_directory, reference_base, reference_catalog))

        if debugger:
            now = debugger['now']
        else:
            now = datetime.datetime.utcnow()

        timestamp = now.strftime(self.__date_format)
        base_name = '%s-%s' % (timestamp, backup_postfix)
        base_directory = os.path.join(self.__backup_directory, base_name)
        base_file = os.path.join(base_directory, base_name)
        catalog_name = '%s-%s' % (timestamp, "catalog")
        catalog_file = os.path.join(base_directory, catalog_name)

        print 'basefile: %s\n' % base_file

        if debugger:
            debugger['directories'].append(base_name)
            debugger['references'].append(reference_base)
        else:
            os.mkdir(base_directory)

            cmd = '%(dar)s --create %(base)s %(reference)s -Q %(options)s' % \
                {'dar': self.__path_to_dar, \
                 'base': self._sanitise_path(base_file), \
                 'reference': reference_option, \
                 'options': self.__backup_options}

            print 'creating backup: %s\n' % cmd
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()
            print

            if retcode == 11:
                self._notify_user('Some files were changed during backup', \
                                      self._WARNING, debugger)
            elif retcode > 0:
                # FIXME: maybe catch exceptions
                # FIXME: delete slices and directory (also in "debugger")
                self._notify_user('dar exited with code %d' % retcode, \
                                      self._ERROR, debugger)

            self._notify_user('%(files)d file(s), %(size)s\n' % \
                                  self.__get_backup_size(base_file), \
                                  self._INFORMATION, debugger)

            # isolate catalog
            cmd = '%(dar)s --isolate %(base)s --ref %(reference)s -Q %(options)s' % \
                {'dar': self.__path_to_dar, \
                 'base': self._sanitise_path(catalog_file), \
                 'reference': self._sanitise_path(base_file), \
                 'options': self.__backup_options}

            print 'isolating catalog: %s\n' % cmd
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()
            print

            if retcode > 0:
                # FIXME: maybe catch exceptions
                # FIXME: delete slices and directory (also in "debugger")
                raise OSError('dar exited with code %d' % retcode)

        self.__delete_old_backups(backup_type, debugger)

        if debugger:
            return

        if self.__backup_database:
            if not os.path.exists(self.__backup_database):
                cmd = '%(dar_manager)s --create %(database)s -Q' % \
                    {'dar_manager': self.__path_to_dar_manager, \
                     'database': self._sanitise_path(self.__backup_database)}

                print 'creating database: %s\n' % cmd
                proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
                proc.communicate()
                retcode = proc.wait()

                if retcode > 0:
                    # FIXME: maybe catch exceptions
                    raise OSError('dar_manager exited with code %d' % retcode)

            cmd = '%(dar_manager)s --base %(database)s --add %(catalog)s %(base)s -Q' % \
                {'dar_manager': self.__path_to_dar_manager, \
                 'database': self._sanitise_path(self.__backup_database), \
                 'catalog': self._sanitise_path(catalog_file), \
                 'base': self._sanitise_path(base_file)}

            print 'updating database: %s\n' % cmd
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()

            if retcode > 0:
                # FIXME: maybe catch exceptions
                self._notify_user('dar_manager exited with code %d' % retcode, \
                                      self._ERROR, debugger)


    def __delete_old_backups(self, backup_type, debugger):
        if backup_type in self.__backup_postfixes:
            backup_postfix = self.__backup_postfixes[backup_type]
        else:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        remove_prior = self.__find_old_backups(backup_type, None, debugger)
        if len (remove_prior) < 2:
            return
        else:
            # get date of previous backup of same type
            prior_date = remove_prior[-2]
            prior_date = prior_date[:-len(backup_postfix) - 1]
            prior_date = datetime.datetime.strptime(prior_date, \
                                                        self.__date_format)

        # please remember: never delete full backups!
        if backup_type == 'full':
            print '\nfull: removing diff and incr prior to last full (%s)\n' % \
                prior_date

            for basename in self.__find_old_backups('incremental', \
                                                        prior_date, debugger):
                self.__delete_backup(basename, debugger)
            for basename in self.__find_old_backups('differential', \
                                                        prior_date, debugger):
                self.__delete_backup(basename, debugger)

            # separate check for old differential backups
            backup_postfix_diff = self.__backup_postfixes['differential']
            remove_prior_diff = self.__find_old_backups( \
                'differential', None, debugger)

            if (len (remove_prior) > 1) and (len(remove_prior_diff) > 0):
                # get date of last full backup
                last_full_date = remove_prior[-1]
                last_full_date = last_full_date[:-len(backup_postfix) - 1]
                last_full_date = datetime.datetime.strptime( \
                    last_full_date, self.__date_format)

                # get date of last differential backup
                last_diff_date = remove_prior_diff[-1]
                last_diff_date = last_diff_date[:-len(backup_postfix_diff) - 1]
                last_diff_date = datetime.datetime.strptime( \
                    last_diff_date, self.__date_format)

                print '\nfull: removing incr prior to last diff (%s)\n' % \
                    last_diff_date

                for basename in self.__find_old_backups( \
                    'incremental', last_diff_date, debugger):
                    self.__delete_backup(basename, debugger)

        elif backup_type == 'differential':
            print '\ndiff: removing incr prior to last diff (%s)\n' % \
                prior_date

            for basename in self.__find_old_backups('incremental', \
                                                        prior_date, debugger):
                self.__delete_backup(basename, debugger)
        elif backup_type == 'incremental':
            return

        self.__remove_empty_directories__()


    def __delete_backup(self, basename, debugger):
        print 'deleting old backup "%s"' % basename
        if debugger:
            n = debugger['directories'].index(basename)
            assert n >= 0

            del debugger['directories'][n]
            del debugger['references'][n]
        else:
            base_directory = os.path.join(self.__backup_directory, basename)
            for backup_file in glob.glob(os.path.join(base_directory, '*.dar')):
                os.unlink(backup_file)

            cmd = '%(dar_manager)s --base %(database)s --list -Q' % \
                {'dar_manager': self.__path_to_dar_manager, \
                 'database': self._sanitise_path(self.__backup_database)}
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE, \
                                        stdout=subprocess.PIPE)
            output = proc.communicate()
            retcode = proc.wait()

            backup_in_database = False
            for line in output[0].split('\n'):
                line = line.strip()
                if line.endswith(basename):
                    regex_match = re.search('^([0-9]+)', line)
                    if regex_match:
                        backup_in_database = True
                        backup_number = regex_match.group(0)
                        print 'updating database (removing backup #%s)' \
                            % backup_number

                        cmd = '%(dar_manager)s --base %(database)s --delete %(number)s -Q' % \
                            {'dar_manager': self.__path_to_dar_manager, \
                             'database': self._sanitise_path( \
                                 self.__backup_database), \
                             'number': backup_number}
                        proc = subprocess.Popen(cmd, shell=True, \
                                                    stdin=subprocess.PIPE)
                        proc.communicate()
                        retcode = proc.wait()

                        if retcode != 0:
                            print 'could not update database'
                        else:
                            print


            if not backup_in_database:
                print 'database not updated (backup not found)\n' \


    def __need_backup(self, debugger):
        for backup_type in self.__backup_types:
            if self.__last_backup(backup_type, debugger) < 0:
                return backup_type

        if self.__force_backup:
            return 'incremental (forced)'
        else:
            return 'none'


    def __last_backup(self, backup_type, debugger):
        if backup_type not in self.__backup_postfixes:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        backup_last = self.__days_since_last_backup(backup_type, debugger)
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


    def __find_old_backups(self, backup_type, prior_date, debugger):
        if backup_type in self.__backup_postfixes:
            backup_postfix = self.__backup_postfixes[backup_type]
        else:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

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
                        catalog_name = '%s-%s.1.dar' % (timestamp, "catalog")
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


    def __name_of_last_backup(self, backup_type, debugger):
        if type(self.__last_backup_file[backup_type]) != types.NoneType:
            return self.__last_backup_file[backup_type]

        directories = self.__find_old_backups(backup_type, None, debugger)
        if len(directories) == 0:
            return None
        else:
            self.__last_backup_file[backup_type] = directories[-1]
            return self.__last_backup_file[backup_type]


    def __days_since_last_backup(self, backup_type, debugger):
        if type(self.__last_backup_days[backup_type]) != types.NoneType:
            return self.__last_backup_days[backup_type]

        most_recent = self.__name_of_last_backup(backup_type, debugger)

        if type(most_recent) == types.NoneType:
            return -1.0
        else:
            if backup_type in self.__backup_postfixes:
                backup_postfix = self.__backup_postfixes[backup_type]
            else:
                raise ValueError('wrong backup type given ("%s")' % backup_type)

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
        if backup_type in self.__backup_postfixes:
            backup_postfix = self.__backup_postfixes[backup_type]
        else:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

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


    def __days_to_next_backup_due_date(self, backup_type, debugger):
        if backup_type not in self.__backup_postfixes:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        remaining_days = -self.__days_since_backup_due_date( \
            backup_type, debugger)

        if remaining_days < 0:
            remaining_days += self.__backup_interval[backup_type]

        return remaining_days


    def __get_backup_size(self, base_file):
        size = 0
        files = 0

        for single_file in glob.glob('%s.*.dar' % base_file):
            if os.path.isfile(single_file) and not os.path.islink(single_file):
                files += 1
                size += os.stat(single_file).st_size

        if size > 1e12:
            filesize = '%s TB' % round(size / 1e12, 1)
        elif size > 1e9:
            filesize = '%s GB' % round(size / 1e9, 1)
        elif size > 1e6:
            filesize = '%s MB' % round(size / 1e6, 1)
        elif size > 1e3:
            filesize = '%s kB' % round(size / 1e3, 1)
        else:
            filesize = '%s bytes' % size

        return {'files': files, 'size': filesize}


    def _sanitise_path(self, original_path):
        if len(original_path) == 0:
            return original_path

        new_path = os.path.abspath(original_path)

        if os.name == 'nt':
            (drive, tail) = os.path.splitdrive(new_path)
            drive = '\\cygdrive\\%s' % drive[0].lower()
            new_path = drive + tail
            new_path = new_path.replace('\\', '/')
            return new_path
        else:
            return new_path


    def _notify_user(self, message, urgency, debugger):
        assert(urgency in (self._INFORMATION, self._WARNING, self._ERROR, \
                               self._FINAL_ERROR))

        if urgency == self._INFORMATION:
            # expire informational messages after 30 seconds
            expiration = 30
        else:
            # do not expire warnings and errors
            expiration = 0

        if (os.name == 'posix') and not debugger:
            cmd = "notify-send -t %(expiration)d -u %(urgency)s -i %(icon)s '%(summary)s' '%(message)s'" % \
                {'expiration': expiration * 1000, \
                 'urgency': 'normal', \
                 'icon': 'dialog-%s' % urgency, \
                 'summary': 'Lalikan (%s)' % self.__section, \
                 'message': message}

            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()

        if urgency == self._ERROR:
            raise OSError(message)
        if urgency == self._FINAL_ERROR:
            print message
            exit(1)
        elif urgency == self._WARNING:
            print 'WARNING: %s' % message
        else:
            print '%s' % message


if __name__ == '__main__':
    lalikan = Lalikan()
    #lalikan.test(60, interval=1.0) # DEBUG
    lalikan.run()
