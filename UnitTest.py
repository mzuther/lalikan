#!/usr/bin/env python3
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

import os.path
import sys
import unittest

import Lalikan.UnitTest.BackupDatabase
import Lalikan.UnitTest.Settings


if __name__ == '__main__':

    def run_tests(module_name, module_import):
        verbosity = 0
        test_runner = unittest.TextTestRunner(verbosity=verbosity)

        print('\n' + module_name)
        eval('test_runner.run({0}.get_suite())'.format(module_import))


    valid_tests = {
        'Lalikan.BackupDatabase': 'Lalikan.UnitTest.BackupDatabase',
        'Lalikan.Settings': 'Lalikan.UnitTest.Settings',
    }

    if 'help' in sys.argv:
        print('\n    Usage: {0} [module1] [module2] [...]'.format(
                os.path.basename(__file__)))
        print('\n    Valid test suites are:\n')
        for key in sorted(valid_tests):
            print('    * ' + key)
        print()
        exit(0)

    specified_tests = sorted(set(sys.argv[1:]))
    for test_suite in specified_tests:
        if test_suite not in valid_tests:
            print('\n    No test suite found in module "{0}".'.format(
                    test_suite))
            print('\n    Valid test suites are:\n')
            for key in sorted(valid_tests):
                print('    * ' + key)
            print()
            exit(1)

    if len(specified_tests) == 0:
        specified_tests = sorted(set(valid_tests))

    for test_suite in specified_tests:
        run_tests(test_suite, valid_tests[test_suite])

    print()
