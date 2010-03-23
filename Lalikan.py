#!/usr/bin/env python2.5
# -*- coding: utf-8 -*-

import datetime
import gettext
import locale
import os
import re
import subprocess
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
        parser.add_option('-s', '--section', \
                              action='store', \
                              dest='section', \
                              metavar='SECTION', \
                              default='default', \
                              help=_('run SECTION from configuration file'))
        parser.add_option('--force', \
                              action='store_true', \
                              dest='force_backup', \
                              default=False, \
                              help=_('force backup'))

        # parse command line
        (options, args) = parser.parse_args()

        self.__section = options.section
        self.__force_backup = options.force_backup

        print settings.get_description(False)
        self.__initialise()


    def __initialise(self):
        self.__backup_client = settings.get( \
            self.__section, 'backup_client', False)
        # port only required for client backup
        self.__backup_client_port = settings.get( \
            self.__section, 'backup_client_port', \
                self.__backup_client == 'localhost')

        self.__backup_directory = settings.get( \
            self.__section, 'backup_directory', False)
        self.__backup_database = settings.get( \
            self.__section, 'backup_database', True)

        self.__backup_interval = { \
            'full': float(settings.get( \
                self.__section, 'backup_interval_full', False)), \
            'differential': float(settings.get( \
                self.__section, 'backup_interval_differential', False)), \
            'incremental': float(settings.get( \
                self.__section, 'backup_interval_incremental', False))}

        self.__backup_options = settings.get( \
            self.__section, 'backup_options', True)

        self.__date_format = settings.get( \
            self.__section, 'date_format', False)
        self.__date_regex = settings.get( \
            self.__section, 'date_regex', False)

        self.__command_pre_run = settings.get( \
            self.__section, 'command_pre_run', True)
        self.__command_post_run = settings.get( \
            self.__section, 'command_post_run', True)

        self.__backup_types = ('full', 'differential', 'incremental')
        self.__backup_prefixes = {'full': 'full_', 'differential': 'diff_', \
                                      'incremental': 'incr_'}
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

            self.__initialise()
            self.__backup_client = 'localhost'

            created_backup = self.run(debugger)

            if not created_backup:
                print
            print '----- %s ------' % debugger['now'].strftime( \
                self.__date_format)
            if created_backup:
                for n in range(len(debugger['directories'])):
                    directory = debugger['directories'][n]
                    reference = debugger['references'][n]

                    if reference.startswith( \
                            self.__backup_prefixes['differential']):
                        reference = '    %s' % reference
                    elif reference.startswith( \
                            self.__backup_prefixes['incremental']):
                        reference = '        %s' % reference

                    if directory.startswith(self.__backup_prefixes['full']):
                        print '%s            %s' % (directory, reference)
                    elif directory.startswith( \
                            self.__backup_prefixes['differential']):
                        print '    %s        %s' % (directory, reference)
                    else:
                        print '        %s    %s' % (directory, reference)
                print '----------------------------\n'


    def run(self, debugger=None):
        if not debugger:
            # check whether the script runs with superuser rights
            if (os.getuid() != 0) or (os.getgid() != 0):
                print '\n%s\n' % \
                    _('You have to run this application with superuser rights.')
                exit(1)

        if not self.__client_is_online():
            return False

        self.__pre_run()

        try:
            need_backup = self.__need_backup(debugger)

            print 'last full:  %7.3f  (%7.3f)  -->  %s' % \
                (self.__last_backup('full', debugger), \
                     self.__backup_interval['full'], \
                     self.__need_backup_type('full', debugger))
            print 'last diff:  %7.3f  (%7.3f)  -->  %s' % \
                (self.__last_backup('differential', debugger), \
                     self.__backup_interval['differential'], \
                     self.__need_backup_type('differential', debugger))
            print 'last incr:  %7.3f  (%7.3f)  -->  %s' % \
                (self.__last_backup('incremental', debugger), \
                     self.__backup_interval['incremental'], \
                     self.__need_backup_type('incremental', debugger))
            print 'backup:     %s' % need_backup

            if need_backup == 'none':
                return False
            elif need_backup == 'incremental (forced)':
                need_backup = 'incremental'

            self.__create_backup(need_backup, debugger)
        finally:
            self.__post_run()

        return True


    def __client_is_online(self):
        # running DAR on localhost -- should be online ... :)
        if self.__backup_client == 'localhost':
            return True

        cmd = ['nc', '-v', '-z', self.__backup_client, \
                   self.__backup_client_port]
        for n in range(3):
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, \
                                        stderr=subprocess.PIPE)
            output = proc.communicate()
            was_successful = output[0].strip().endswith(' succeeded!')
            error_string = (len(output[1]) > 0)
            if was_successful and not error_string:
                return True

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
                        print _('removed empty directory "%(directory)s".') % \
                            {'directory':root}
                        # delete empty directory
                        os.rmdir(root)


    def __pre_run(self):
        if self.__command_pre_run:
            print 'pre-run command: %s' % self.__command_pre_run
            proc = subprocess.Popen( \
                self.__command_pre_run, shell=True, stdin=subprocess.PIPE, \
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                print output[0]
            if output[1]:
                raise OSError(output[1])

        self.__remove_empty_directories__()


    def __post_run(self):
        if self.__command_post_run:
            print 'post-run command: %s' % self.__command_post_run
            proc = subprocess.Popen( \
                self.__command_post_run, shell=True, stdin=subprocess.PIPE, \
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            output = proc.communicate()
            if output[0]:
                print output[0]
            if output[1]:
                raise OSError(output[1])


    def __create_backup(self, backup_type, debugger):
        print
        if backup_type not in self.__backup_prefixes:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        backup_prefix = self.__backup_prefixes[backup_type]

        if backup_type == 'full':
            reference = 'none'
            reference_option = ''
        elif backup_type == 'differential':
            reference = self.__name_of_last_backup('full', debugger)
            reference_option = '--ref ' + os.path.join( \
                self.__backup_directory, reference, reference)
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

            reference = self.__name_of_last_backup(newest_backup, debugger)
            reference_option = '--ref ' + os.path.join( \
                self.__backup_directory, reference, reference)
        if debugger:
            now = debugger['now']
        else:
            now = datetime.datetime.utcnow()

        timestamp = now.strftime(self.__date_format)
        base_directory = os.path.join(self.__backup_directory, \
                                          backup_prefix + timestamp)
        base_file = os.path.join(base_directory, backup_prefix + timestamp)

        print 'basefile: %s\n' % base_file

        if debugger:
            debugger['directories'].append(backup_prefix + timestamp)
            debugger['references'].append(reference)
            return

        os.mkdir(base_directory)

        cmd = 'dar --create %(base)s %(reference)s -Q %(options)s' % \
            {'base': base_file, 'reference': reference_option, \
                 'options': self.__backup_options}

        print 'creating backup: %s\n' % cmd
        proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
        proc.communicate()
        retcode = proc.wait()
        print

        if retcode > 0:
            # FIXME: maybe catch exceptions
            # FIXME: delete slices and directory (also in "debugger")
            raise OSError('dar exited with code %d' % retcode)

        if self.__backup_database:
            if not os.path.exists(self.__backup_database):
                cmd = 'dar_manager --create %(database)s -Q' % \
                    {'database': self.__backup_database}

                print 'creating database: %s\n' % cmd
                proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
                proc.communicate()
                retcode = proc.wait()

                if retcode > 0:
                    # FIXME: maybe catch exceptions
                    raise OSError('dar_manager exited with code %d' % retcode)

            cmd = 'dar_manager --base %(database)s --add %(base)s -Q' % \
                {'database': self.__backup_database, 'base': base_file}

            print 'updating database: %s\n' % cmd
            proc = subprocess.Popen(cmd, shell=True, stdin=subprocess.PIPE)
            proc.communicate()
            retcode = proc.wait()

            if retcode > 0:
                # FIXME: maybe catch exceptions
                raise OSError('dar_manager exited with code %d' % retcode)


    def __need_backup(self, debugger):
        for backup_type in self.__backup_types:
            if self.__need_backup_type(backup_type, debugger):
                return backup_type
        if self.__force_backup:
            return 'incremental (forced)'
        else:
            return 'none'


    def __need_backup_type(self, backup_type, debugger):
        last_backup = self.__last_backup(backup_type, debugger)
        if last_backup < 0:
            return True
        else:
            if (self.__backup_interval[backup_type] <= \
                    self.__last_backup(backup_type, debugger)):
                return True
            else:
                return False


    def __last_backup(self, backup_type, debugger):
        if backup_type == 'full':
            full = self.__days_since_last_backup('full', debugger)
            return full
        elif backup_type == 'differential':
            # recursive call!
            full = self.__last_backup('full', debugger)
            diff = self.__days_since_last_backup('differential', debugger)
            if (diff < 0) or (full < diff):
                return full
            else:
                return diff
        elif backup_type == 'incremental':
            # recursive call!
            diff = self.__last_backup('differential', debugger)
            incr = self.__days_since_last_backup('incremental', debugger)
            if (incr < 0) or (diff < incr):
                return diff
            else:
                return incr
        else:
            raise ValueError('wrong backup type given ("%s")' % backup_type)


    def __name_of_last_backup(self, backup_type, debugger):
        if type(self.__last_backup_file[backup_type]) != types.NoneType:
            return self.__last_backup_file[backup_type]

        if backup_type in self.__backup_prefixes:
            backup_prefix = self.__backup_prefixes[backup_type]
        else:
            raise ValueError('wrong backup type given ("%s")' % backup_type)

        regex = re.compile('^%s%s$' % (backup_prefix, self.__date_regex))
        directories = []
        if debugger:
            for item in debugger['directories']:
                if regex.match(item):
                    directories.append(item)
        else:
            for item in os.listdir(self.__backup_directory):
                item_full = os.path.join(self.__backup_directory, item)
                if os.path.isdir(item_full):
                    if regex.match(item):
                        directories.append(item)

        directories.sort()
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
            if backup_type in self.__backup_prefixes:
                backup_prefix = self.__backup_prefixes[backup_type]
            else:
                raise ValueError('wrong backup type given ("%s")' % backup_type)

            most_recent = most_recent[len(backup_prefix):]
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


if __name__ == '__main__':
    lalikan = Lalikan()
    # lalikan.test(31, interval=1.0)
    lalikan.run()
