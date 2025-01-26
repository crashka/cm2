# -*- coding: utf-8 -*-

"""Core configuration and definitions for database stuff.
"""

from datetime import datetime, date

from peewee import Model, DateTimeField
from playhouse.sqlite_ext import SqliteExtDatabase

from .core import cfg, DataFile, ConfigError

DFLT_DB    = 'cm2.sqlite'
DB_KEY     = 'databases'
SQLITE_KEY = 'sqlite'

db_config = cfg.config(DB_KEY)
if not db_config or SQLITE_KEY not in db_config:
    raise ConfigError("'{DB_KEY}' or '{SQLITE_KEY}' not found in config file")
SQLITE = db_config.get(SQLITE_KEY)

#####################
# Utility Functions #
#####################

def now_str() -> str:
    """ Need this to make model objects serializable as JSON
    """
    return str(datetime.now())

def date_str(ts: int = None) -> str:
    """ Need this to make model objects serializable as JSON
    """
    dt = date.fromtimestamp(ts) if ts else date.today()
    return str(dt)

#############
# BaseModel #
#############

pragmas = {'journal_mode'            : 'wal',
           'cache_size'              : -1 * 64000,  # 64MB
           'foreign_keys'            : 1,
           'ignore_check_constraints': 0,
           'synchronous'             : 0}

db_file = SQLITE.get('db_file') or DFLT_DB
db = SqliteExtDatabase(DataFile(db_file), pragmas)

class BaseModel(Model):
    """Base model for this module, with defaults and system columns
    """
    # system columns
    created_at    = DateTimeField(default=now_str)
    updated_at    = DateTimeField()

    def save(self, *args, **kwargs):
        if not self.updated_at:
            self.updated_at = self.created_at
        elif 'updated_at' not in self._dirty:
            self.updated_at = now_str()
        return super().save(*args, **kwargs)

    class Meta:
        database = db
        legacy_table_names = False
