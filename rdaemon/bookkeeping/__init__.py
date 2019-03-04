"""
Author: Liran Funaro <funaro@cs.technion.ac.il>

Copyright (C) 2006-2018 Liran Funaro

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""
import sys
import pkgutil
import importlib
import warnings

from typing import Optional


DEFAULT_BOOKKEEPING = 'file'
BOOKKEEPINGS = set([modname for _, modname, _ in pkgutil.iter_modules(sys.modules[__name__].__path__)])


def assert_valid_bookkeeping(bookkeeping_method: Optional[str] = None):
    if bookkeeping_method is None:
        return

    if not isinstance(bookkeeping_method, str):
        raise TypeError(f"Bookkeeping method must be a string. Not {bookkeeping_method}. "
                        f"Choose one of the followings: {BOOKKEEPINGS}.")

    if bookkeeping_method not in BOOKKEEPINGS:
        raise ValueError(f"No such bookkeeping method: {bookkeeping_method}. "
                         f"Choose one of the followings: {BOOKKEEPINGS}.")


def daemonize(daemon_name: str, sub_path: Optional[str], bookkeeping_method: Optional[str] = None):
    """
    The target function of the new process.
    """
    if bookkeeping_method is None:
        bookkeeping_method = DEFAULT_BOOKKEEPING
    assert_valid_bookkeeping(bookkeeping_method)

    with warnings.catch_warnings():
        warnings.simplefilter('ignore', category=ImportWarning)
        bookkeeping_module = importlib.import_module(f'.{bookkeeping_method}', __name__)

    return bookkeeping_module.daemonize(daemon_name=daemon_name, sub_path=sub_path)
