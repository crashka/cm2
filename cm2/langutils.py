# -*- coding: utf-8 -*-

"""Miscellaneous language processing functions.
"""

from unidecode import unidecode

def count_non_ascii(s: str) -> int:
    """Count the number of non-ascii characters in the input string.
    """
    return len([c for c in s if ord(c) > 0x7f])

def norm(s: str) -> str:
    """Normalize string for case-insensitive cross-alphabet matching (i.e. remove accents
    and diacritics, as well as downcase).
    """
    return unidecode(s).lower()
