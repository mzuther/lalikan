#!/usr/bin/env python3

"""Lalikan
   =======
   Backup scheduler for Disk ARchive (DAR)

   Copyright (c) 2010-2015 Dr. Martin Zuther (http://www.mzuther.de/)

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

import argparse
import os
import sys

import lalikan.settings
import lalikan.runner


def check_requirements():
    """
    Check application requirements.  Throws an exception should
    requirements not be fulfilled.

    :rtype:
        None

    """
    # check Python version
    if sys.version_info.major != 3:
        raise NotImplementedError('Lalikan does not run on Python {}.'.format(
            sys.version_info.major))


def print_header():
    """
    Print application name and version on command line.

    :rtype:
        None

    """
    # get application name and version
    name_and_version = settings.get_name_and_version()

    print()
    print(name_and_version)
    print('=' * len(name_and_version))

    # on Linux, check whether the script runs with superuser rights
    if sys.platform == 'linux' and os.getuid() != 0:
        print('You probably want to run this application with '
              'superuser rights...')

    print()


def list_sections(message):
    """
    Print defined message and defined backup sections on command line.

    :param message:
        message to be displayed on command line
    :type settings:
        String

    :rtype:
        None

    """
    print()
    print(message)
    print()

    # loop over defined sections
    for section in settings.sections():
        print(' * ' + section)

    print()


def parse_command_line(settings):
    """
    Parse command line.

    :param settings:
        backup settings and application information
    :type settings:
        lalikan.settings

    :returns:
        selected backup section (**None** for all sections) and
        whether a backup should be forced
    :rtype:
        tuple(section, force_backup)

    """
    # initialise command line parser
    parser = argparse.ArgumentParser(
        description=settings.get_description(),
        formatter_class=argparse.RawDescriptionHelpFormatter)

    # argument: show version information
    parser.add_argument(
        '--version',
        action='version',
        version=settings.get_name_and_version())

    # argument: show copyright and licence information
    parser.add_argument(
        '--licence',
        action='store_true',
        dest='licence',
        default=False,
        help='show copyright and licence information and exit')

    # argument: list all backup sections
    parser.add_argument(
        '-l', '--list',
        action='store_true',
        dest='list_sections',
        default=False,
        help='list all defined backup sections and exit')

    # argument: create backup for section only
    parser.add_argument(
        '-s', '--section',
        action='store',
        dest='section',
        metavar='SECTION',
        default=None,
        help='create backup for SECTION (otherwise, backups for all '
             'sections will be created)')

    # argument: force backup
    parser.add_argument(
        '--force',
        action='store_true',
        dest='force_backup',
        default=False,
        help='force backup')

    # parse command line
    args = parser.parse_args()

    # show copyright and licence information
    if args.licence:
        # get application name and version
        name_and_version = settings.get_name_and_version()

        print()
        print(name_and_version)
        print('=' * len(name_and_version))

        print(settings.get_description())
        print()

        print(settings.get_copyrights())
        print()

        print(settings.get_license(True))
        print()

        exit(0)

    # list defined sections and exit
    if args.list_sections:
        list_sections('Backup sections:')
        exit(0)

    # user asked to create a specific backup, ...
    if args.section is not None:
        # ..., but the specified backup section does not exist
        if args.section not in settings.sections():
            # print error message, list defined sections and exit
            message = 'Could not find section "{}".  '.format(args.section)
            message += 'Please use one of these sections:'

            list_sections(message)
            exit(1)

    return (args.section, args.force_backup)


if __name__ == '__main__':
    # check Python version
    try:
        check_requirements()
    except Exception as err:
        print()
        print(err)
        print()

        exit(1)

    # load Lalikan settings
    settings = lalikan.settings.Settings('/etc/lalikan')

    # parse command line
    section, force_backup = parse_command_line(settings)

    # print application name and version
    print_header()

    # create backup for all sections
    if section is None:
        sections = settings.sections()
    # create backup for specified section only
    else:
        sections = [section]

    # keep track of backup errors
    errors_occurred = False

    # loop over specified backup sections
    for n in range(len(sections)):
        try:
            # create backup for section
            lalikan.runner.BackupRunner(settings, sections[n], force_backup)
        except OSError as err:
            # print error message
            print(err)

            # remember that an error occurred
            errors_occurred = True
        finally:
            if n < (len(sections) - 1):
                print()
                print('---')

            print()

    # print summary and exit with error code
    if errors_occurred:
        print('---')
        print()
        print('At least one error has occurred!')
        print()
        exit(1)
