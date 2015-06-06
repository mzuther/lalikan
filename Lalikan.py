#!/usr/bin/env python3

"""Lalikan
   =======
   Backup scheduler for Disk ARchive (DAR)

   Copyright (c) 2010-2015 Martin Zuther (http://www.mzuther.de/)

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
import subprocess
import sys
import time

from optparse import OptionParser

import Lalikan.BackupDatabase
from Lalikan.Settings import *

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
                       {'description': settings.get_description(True),
                        'copyrights': settings.get_copyrights(),
                        'license': settings.get_license(True)}
        # ... usage information and ...
        usage = 'Usage: %(cmd_line)s [options]' % \
            {'cmd_line': settings.get_option('cmd_line')}
        # ... the command line parser itself
        parser = OptionParser(usage=usage, version=version_long)

        # add command line options
        parser.add_option('-l', '--list',
                          action='store_true',
                          dest='list_sections',
                          default=False,
                          help=_('list all sections defined in the configuration file'))

        parser.add_option('-s', '--section',
                          action='store',
                          dest='section',
                          metavar='SECTION',
                          default=None,
                          help=_('run SECTION from configuration file'))

        parser.add_option('--force',
                          action='store_true',
                          dest='force_backup',
                          default=False,
                          help=_('force backup'))

        # parse command line
        (options, args) = parser.parse_args()

        print(settings.get_description(False))

        self.__section = options.section
        self.__force_backup = options.force_backup

        if options.list_sections:
            print('The following sections have been defined in the configuration file:\n')

            for section in settings.sections():
                print(' * %s' % section)

            print()
            exit(0)

        if (self.__section not in settings.sections()) and \
                (self.__section is not None):
            print('The specified section \'%s\' has not been defined.  Please use one of these:\n' % \
                options.section)

            for section in settings.sections():
                print(' * %s' % section)

            print()
            exit(1)


    def test(self, number_of_days, interval=1.0):
        current_day = 0.0
        debugger = {'directories': [], 'references': [],
                    'now': datetime.datetime.now()}

        while current_day <= number_of_days:
            if current_day > 0.0:
                debugger['now'] += datetime.timedelta(interval)
            current_day += interval

            created_backup = self.run(debugger)

            if not created_backup:
                print()
            print('----- %s ------' % debugger['now'].strftime(
                self.db.date_format))
            if created_backup:
                for n in range(len(debugger['directories'])):
                    directory = debugger['directories'][n]
                    reference = debugger['references'][n]

                    if reference.endswith(
                            self.db.backup_postfix_diff):
                        reference = '    %s' % reference
                    elif reference.endswith(
                            self.db.backup_postfix_incr):
                        reference = '        %s' % reference

                    if directory.endswith(
                            self.db.backup_postfix_full):
                        print('%s            %s' % (directory, reference))
                    elif directory.endswith(
                            self.db.backup_postfix_diff):
                        print('    %s        %s' % (directory, reference))
                    else:
                        print('        %s    %s' % (directory, reference))
                print('----------------------------\n')


    def run(self, debugger=None):
        if debugger:
            try:
                if self.__section is None:
                    self.__run(settings.sections()[0], debugger)
                else:
                    self.__run(self.__section, debugger)
            except OSError as err:
                print(err)
                exit(1)
        elif self.__section is None:
            for self.__section in settings.sections():
                error = False

                try:
                    self.__run(self.__section, debugger)
                except OSError as err:
                    error = True
                    print(err)

                    if error:
                        self.__notify_user('At least one error has occurred!',
                                           self._FINAL_ERROR, debugger)
        elif self.__section in settings.sections():
            try:
                self.__run(self.__section, debugger)
            except OSError as err:
                print(err)
                exit(1)
        else:
            exit(1)


    def __run(self, section, debugger=None):
        self.db = Lalikan.BackupDatabase.BackupDatabase(
            section, settings, debugger)

        if not debugger and (sys.platform == 'linux'):
            # check whether the script runs with superuser rights
            if (os.getuid() != 0) or (os.getgid() != 0):
                print('\n%s\n' % \
                    _('You have to run this application with superuser rights.'))
                exit(1)

        if not debugger:
            self.__pre_run(section, debugger)

        need_backup = 'none'
        try:
            need_backup = self.db.need_backup(self.__force_backup, debugger)

            print('\nnext full in  %7.3f days  (%7.3f)' % \
                (self.db.days_to_next_backup_due_date('full', debugger),
                 self.db.backup_interval_full))
            print('next diff in  %7.3f days  (%7.3f)' % \
                (self.db.days_to_next_backup_due_date('differential', debugger),
                 self.db.backup_interval_diff))
            print('next incr in  %7.3f days  (%7.3f)\n' % \
                (self.db.days_to_next_backup_due_date('incremental', debugger),
                 self.db.backup_interval_incr))

            print('backup type:  %s\n' % need_backup)

            if need_backup == 'none':
                return False
            elif need_backup == 'incremental (forced)':
                need_backup = 'incremental'

            if not debugger:
                self.__notify_user('Starting %s backup...' % need_backup,
                                   self._INFORMATION, debugger)

            self.__create_backup(need_backup, debugger)
        finally:
            if not debugger:
                self.__post_run(section, need_backup, debugger)

        return True


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
            for root, directories, files in os.walk(
                    self.db.backup_directory, topdown=False):
               # do not remove root directory
                if root != self.db.backup_directory:
                    # directory is empty
                    if (len(directories) < 1) and (len(files) < 1):
                        # keep looping to find *all* empty directories
                        repeat = True
                        print(_('removing empty directory "%(directory)s"') % \
                            {'directory': root})
                        # delete empty directory
                        os.rmdir(root)


    def __pre_run(self, section, debugger=None):
        if self.db.pre_run_command:
            print('pre-run command: %s' % self.db.pre_run_command)
            # stdin is needed to be able to communicate with the
            # application (i.e. answer a question)
            proc = subprocess.Popen(
                self.db.pre_run_command, shell=True,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                self.__notify_user(output[0], self._INFORMATION, debugger)
            if output[1]:
                self.__notify_user(output[1], self._ERROR, debugger)

        # recursively create root directory if it doesn't exist
        if not os.path.exists(self.db.backup_directory):
            os.makedirs(self.db.backup_directory)

        self.__remove_empty_directories__()


    def __post_run(self, section, need_backup, debugger=None):
        if self.db.post_run_command:
            print('post-run command: %s' % \
                self.db.post_run_command)
            proc = subprocess.Popen(
                self.db.post_run_command, shell=True,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                self.__notify_user(output[0], self._INFORMATION, debugger)
            if output[1]:
                self.__notify_user(output[1], self._ERROR, debugger)

        if need_backup != 'none':
            self.__notify_user('Finished backup.', self._INFORMATION, debugger)

        print('---')


    def __create_backup(self, backup_type, debugger):
        print()
        self.db.check_backup_type(backup_type)

        if backup_type == 'full':
            backup_postfix = self.db.backup_postfix_full
            reference_base = 'none'
            reference_option = ''
        elif backup_type == 'differential':
            backup_postfix = self.db.backup_postfix_diff
            reference_base = self.db.name_of_last_backup('full', debugger)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '%s-%s' % (reference_timestamp, "catalog")
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self.db.backup_directory, reference_base,
                reference_catalog))
        elif backup_type == 'incremental':
            backup_postfix = self.db.backup_postfix_incr
            last_full = self.db.days_since_last_backup(
                'full', debugger)
            last_differential = self.db.days_since_last_backup(
                'differential', debugger)
            last_incremental = self.db.days_since_last_backup(
                'incremental', debugger)

            newest_backup = 'full'
            newest_age = last_full

            if (last_differential >= 0) and (last_differential < newest_age):
                newest_backup = 'differential'
                newest_age = last_differential

            if (last_incremental >= 0) and (last_incremental < newest_age):
                newest_backup = 'incremental'
                newest_age = last_incremental

            reference_base = self.db.name_of_last_backup(
                newest_backup, debugger)
            reference_timestamp = reference_base.rsplit('-', 1)[0]
            reference_catalog = '%s-%s' % (reference_timestamp, "catalog")
            reference_option = '--ref ' + self.sanitise_path(os.path.join(
                self.db.backup_directory, reference_base,
                reference_catalog))

        if debugger:
            now = debugger['now']
        else:
            now = datetime.datetime.now()

        timestamp = now.strftime(self.db.date_format)
        base_name = '%s-%s' % (timestamp, backup_postfix)
        base_directory = os.path.join(self.db.backup_directory, base_name)
        base_file = os.path.join(base_directory, base_name)
        catalog_name = '%s-%s' % (timestamp, "catalog")
        catalog_file = os.path.join(base_directory, catalog_name)

        print('basefile: %s\n' % base_file)

        if debugger:
            debugger['directories'].append(base_name)
            debugger['references'].append(reference_base)
        else:
            os.mkdir(base_directory)

            cmd = '%(dar)s --create %(base)s %(reference)s -Q %(options)s' % \
                  {'dar': self.db.path_to_dar,
                   'base': self.sanitise_path(base_file),
                   'reference': reference_option,
                   'options': self.db.backup_options}

            print('creating backup: %s\n' % cmd)
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()
            print()

            if retcode == 11:
                self.__notify_user('Some files were changed during backup.',
                                   self._WARNING, debugger)
            elif retcode > 0:
                # FIXME: maybe catch exceptions
                # FIXME: delete slices and directory (also in "debugger")
                self.__notify_user('dar exited with code %d.' % retcode,
                                   self._ERROR, debugger)

            self.__notify_user('%(files)d file(s), %(size)s\n' % \
                               self.__get_backup_size(base_file),
                               self._INFORMATION, debugger)

            # isolate catalog
            cmd = '%(dar)s --isolate %(base)s --ref %(reference)s -Q %(options)s' % \
                  {'dar': self.db.path_to_dar,
                   'base': self.sanitise_path(catalog_file),
                   'reference': self.sanitise_path(base_file),
                   'options': self.db.backup_options}

            print('isolating catalog: %s\n' % cmd)
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()
            print()

            if retcode == 5:
                self.__notify_user('Some files do not follow chronological order when archive index increases.',
                                   self._WARNING, debugger)
            elif retcode > 0:
                # FIXME: maybe catch exceptions
                # FIXME: delete slices and directory (also in "debugger")
                raise OSError('dar exited with code %d' % retcode)

        self.__delete_old_backups(backup_type, debugger)


    def __delete_old_backups(self, backup_type, debugger):
        self.db.check_backup_type(backup_type)

        if backup_type == 'full':
            backup_postfix = self.db.backup_postfix_full
        elif backup_type == 'differential':
            backup_postfix = self.db.backup_postfix_diff
        elif backup_type == 'incremental':
            backup_postfix = self.db.backup_postfix_incr

        remove_prior = self.db.find_old_backups(backup_type, None, debugger)
        if len (remove_prior) < 2:
            return
        else:
            # get date of previous backup of same type
            prior_date = remove_prior[-2]
            prior_date = prior_date[:-len(backup_postfix) - 1]
            prior_date = datetime.datetime.strptime(
                prior_date, self.db.date_format)

        # please remember: never delete full backups!
        if backup_type == 'full':
            print('\nfull: removing diff and incr prior to last full (%s)\n' % \
                prior_date)

            for basename in self.db.find_old_backups(
                    'incremental', prior_date, debugger):
                self.__delete_backup(basename, debugger)
            for basename in self.db.find_old_backups(
                    'differential', prior_date, debugger):
                self.__delete_backup(basename, debugger)

            # separate check for old differential backups
            backup_postfix_diff = self.db.backup_postfix_diff
            remove_prior_diff = self.db.find_old_backups(
                'differential', None, debugger)

            if (len (remove_prior) > 1) and (len(remove_prior_diff) > 0):
                # get date of last full backup
                last_full_date = remove_prior[-1]
                last_full_date = last_full_date[:-len(backup_postfix) - 1]
                last_full_date = datetime.datetime.strptime(
                    last_full_date, self.db.date_format)

                # get date of last differential backup
                last_diff_date = remove_prior_diff[-1]
                last_diff_date = last_diff_date[:-len(backup_postfix_diff) - 1]
                last_diff_date = datetime.datetime.strptime(
                    last_diff_date, self.db.date_format)

                print('\nfull: removing incr prior to last diff (%s)\n' % \
                    last_diff_date)

                for basename in self.db.find_old_backups(
                        'incremental', last_diff_date, debugger):
                    self.__delete_backup(basename, debugger)

        elif backup_type == 'differential':
            print('\ndiff: removing incr prior to last diff (%s)\n' % \
                prior_date)

            for basename in self.db.find_old_backups(
                    'incremental', prior_date, debugger):
                self.__delete_backup(basename, debugger)
        elif backup_type == 'incremental':
            return

        self.__remove_empty_directories__()


    def __delete_backup(self, basename, debugger):
        print('deleting old backup "%s"' % basename)
        if debugger:
            n = debugger['directories'].index(basename)
            assert n >= 0

            del debugger['directories'][n]
            del debugger['references'][n]
        else:
            base_directory = os.path.join(self.db.backup_directory, basename)
            for backup_file in glob.glob(os.path.join(base_directory, '*.dar')):
                os.unlink(backup_file)
            for checksum_file in glob.glob(os.path.join(base_directory, '*.dar.md5')):
                os.unlink(checksum_file)


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


    def sanitise_path(self, path):
        return self.db.sanitise_path(path)


    def __notify_user(self, message, urgency, debugger):
        assert(urgency in (self._INFORMATION, self._WARNING, self._ERROR,
                           self._FINAL_ERROR))

        if urgency == self._INFORMATION:
            # expire informational messages after 30 seconds
            expiration = 30
        else:
            # do not expire warnings and errors
            expiration = 0

        if not debugger and (sys.platform == 'linux'):
            cmd = "notify-send -t %(expiration)d -u %(urgency)s -i %(icon)s '%(summary)s' '%(message)s'" % \
                  {'expiration': expiration * 1000,
                   'urgency': 'normal',
                   'icon': 'dialog-%s' % urgency,
                   'summary': 'Lalikan (%s)' % self.__section,
                   'message': message}

            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()

        if urgency == self._ERROR:
            raise OSError(message)
        if urgency == self._FINAL_ERROR:
            print(message)
            exit(1)
        elif urgency == self._WARNING:
            print('WARNING: %s' % message)
        else:
            print('%s' % message)


if __name__ == '__main__':
    try:
        if sys.version_info.major != 3:
            error_string = 'Lalikan does not run on Python {0}.'
            raise EnvironmentError(error_string.format(sys.version_info.major))

        raise EnvironmentError('Lalikan is not ready for Python 3 yet.')

        lalikan = Lalikan()

        DEBUG = False
        if DEBUG:
            lalikan.test(60, interval=1.0)
        else:
            lalikan.run()

    except EnvironmentError as err:
        print('\n  {0}\n'.format(err))
        exit(1)
