# -*- coding: utf-8 -*-

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

import collections
import functools
import gettext
import os

# initialise localisation settings
module_path = os.path.dirname(os.path.realpath(__file__))
gettext.bindtextdomain('Lalikan', os.path.join(module_path, 'po/'))
gettext.textdomain('Lalikan')
_ = gettext.lgettext


# @Memoized decorator; please note that it ignores **kwargs!  Adapted
# from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
class Memoized(object):
   '''Decorator. Caches a function's return value each time it is called.
      If called later with the same arguments, the cached value is
      returned rather than computing the result again.

   '''
   def __init__(self, func):
      self.func = func
      self.clear_cache()


   def __call__(self, *args):
      # Uncacheable, for instances a list.  Better to not cache
      # than to blow up...
      if not isinstance(args, collections.Hashable):
         return self.func(*args)

      # Result already in cache
      if args in self.cache:
         self.hits += 1
         return self.cache[args]
      # Calculate result and store in cache
      else:
         self.misses += 1

         value = self.func(*args)
         self.cache[args] = value
         return value


   def __get__(self, obj, objtype):
      '''Support instance methods.

      '''
      return functools.partial(self.__call__, obj)


   def __str__(self):
      return '{hits} hits, {misses} misses.'.format(**self.cache_info())


   def cache_info(self):
      return {'hits': self.hits, 'misses': self.misses}


   def clear_cache(self):
      self.cache = {}
      self.hits = 0
      self.misses = 0
