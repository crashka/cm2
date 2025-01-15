#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schema entity definitions and management commands.
"""

from peewee import *
from playhouse.sqlite_ext import *

from .core import log
from .dbcore import db, BaseModel

##########
# Person #
##########

# Note: "comp" in this module means component (not composer)
NAME_COMPS     = ['title', 'first_name', 'middle_name', 'last_prefix', 'last_name', 'suffix']
ALT_NAME_COMPS = ['last_name', 'suffix', 'title', 'first_name', 'middle_name', 'last_prefix']

class Person(BaseModel):
    """Represents a person
    """
    name          = TextField()           # defaults to full_name
    disamb        = TextField(default='')
    alt_name      = TextField(null=True)  # generally leads with last name

    # name components ("generally" normalized)
    title         = TextField(null=True)
    first_name    = TextField(null=True)
    middle_name   = TextField(null=True)
    last_prefix   = TextField(null=True)
    last_name     = TextField(null=True)
    suffix        = TextField(null=True)

    # denormalized flags (from `tags` and joins)
    is_composer   = BooleanField(null=True)
    is_conductor  = BooleanField(null=True)
    is_performer  = BooleanField(null=True)

    # NOTE: canonical record is assumed to be normalized and authoritative
    is_canonical  = BooleanField(null=True)
    cnl_person_id = ForeignKeyField('self', null=True, backref='aliases')  # points to self, if canonical

    # reference info
    tags          = JSONField(default=[])  # JSON array of strings
    notes         = JSONField(default=[])  # JSON array of strings
    born          = TextField(null=True)
    died          = TextField(null=True)
    country       = TextField(null=True)
    epoch         = TextField(null=True)
    source        = TextField(null=True)
    source_date   = DateField(null=True)
    arkiv_uri     = TextField(null=True)

    class Meta:
        indexes = (
            # duplicate names must be disambiguatable
            (('name', 'disamb'), True),
        )

    @property
    def full_name(self) -> str:
        """ Construct full name from individual name components.
        """
        comps = [getattr(self, x) for x in NAME_COMPS if getattr(self, x)]
        # add comma before name suffix, if exists
        if self.suffix:
            assert len(comps) > 1
            comps[-2] += ','
        return ' '.join(comps)

    def mk_alt_name(self) -> str:
        """ Alternate construction of person's name, leading with last name.
        """
        comps = [getattr(self, x) for x in ALT_NAME_COMPS if getattr(self, x)]
        # add comma after last name, or suffix (if exists)
        if self.last_name and len(comps) > 1:
            if not self.suffix:
                comps[0] += ','
            elif self.suffix and len(comps) > 2:
                comps[1] += ','
        return ' '.join(comps)

    def save(self, *args, **kwargs):
        if not self.name:
            self.name = self.full_name
        if not self.alt_name:
            alt_name = self.mk_alt_name()
            if alt_name != self.name:
                self.alt_name = alt_name
        return super().save(*args, **kwargs)

##############
# PersonMeta #
##############

class PersonMeta(BaseModel):
    """Represents metainformation (additional field values) for a person
    """
    person        = ForeignKeyField(Person, backref='person_metas')
    key           = TextField()
    value         = TextField(null=True)
    source        = TextField(null=True)
    source_date   = DateField(null=True)

    class Meta:
        indexes = (
            # REVISIT: should we add source_date (otherwise need to think about conflict
            # handling logic)!!!
            (('person', 'key', 'source'), True),
        )

##############
# PersonName #
##############

class PersonName(BaseModel):
    """Represents a person name
    """
    name_str      = TextField()            # raw name string (no fixup)
    source        = TextField(null=True)
    source_date   = DateField(null=True)
    person        = ForeignKeyField(Person, null=True, backref='person_names')
    person_res    = TextField(null=True)   # person resolution mechanism (or process?)

    class Meta:
        indexes = (
            # REVISIT: should we add source_date (otherwise need to think about conflict
            # handling logic)!!!
            (('name_str', 'source'), True),
        )

############
# Conflict #
############

class Conflict(BaseModel):
    """Represents entity conflicts (on unique keys) to be resolved
    """
    entity_name = TextField()  # raw name string (no fixup)
    entity_def  = JSONField()  # JSON object of entity attributes (including metainfo)
    status      = TextField(default='open')  # 'open', 'in process, 'resolved', 'withdrawn'
    parent_id   = IntegerField(null=True)    # id of parent (resolved to) record

    class Meta:
        indexes = (
            # guard against duplicate records
            (('entity_name', 'entity_def'), True),
        )

##########
# create #
##########

ALL_MODELS = [Person,
              PersonMeta,
              PersonName,
              Conflict]

def create(models: list[str] | str = 'all', force: bool = False, **kwargs) -> None:
    """Create tables for the specified schema models.
    """
    if kwargs:
        raise RuntimeError(f"Unexpected argument(s): {', '.join(kwargs.keys())}")

    if isinstance(models, str):
        if models == 'all':
            models = ALL_MODELS
        else:
            models = models.split(',')
    assert isinstance(models, list)
    if isinstance(models[0], str):
        models_new = []
        for model in models:
            if model not in globals():
                raise RuntimeError(f"Model {model} not imported")
            model_obj = globals()[model]
            if not issubclass(model_obj, BaseModel):
                raise RuntimeError(f"Model {model} must be subclass of `BaseModel`")
            models_new.append(model_obj)
        models = models_new

    if db.is_closed():
        db.connect()
    for model in models:
        try:
            model.create_table(safe=False)
            log.info(f"Created table {model._meta.table_name}")
        except OperationalError as e:
            if re.fullmatch(r'table "(\w+)" already exists', str(e)) and force:
                model.drop_table(safe=False)
                model.create_table(safe=False)
                log.info(f"Re-created table {model._meta.table_name}")
            else:
                raise

########
# main #
########

import sys

from ckautils import parse_argv

ACTIONS = {'create': create}

def main() -> int:
    """Usage::

      $ python -m schema <action> [<arg>=<val> [...]]

    Actions (and associated arguments):

      - create models=<models> force=<bool>

    where:

      - <models> is a comma-separate list of model names (case-sensitive), or 'all'
      - `force` (optional) is false by default

    """
    if len(sys.argv) < 2:
        print(f"Action not specified", file=sys.stderr)
        return -1
    elif sys.argv[1] not in ACTIONS:
        print(f"Unknown utility function '{sys.argv[1]}'", file=sys.stderr)
        return -1

    util_func = ACTIONS[sys.argv[1]]
    args, kwargs = parse_argv(sys.argv[2:])
    if args:
        print(f"Unexpected argument(s): {', '.join(args)}", file=sys.stderr)
        return -1

    util_func(**kwargs)  # any return (no exception) is considered success
    return 0

if __name__ == '__main__':
    sys.exit(main())
