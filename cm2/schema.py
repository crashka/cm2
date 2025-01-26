#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Schema entity definitions and management commands.
"""

from enum import StrEnum

from peewee import *
from playhouse.sqlite_ext import *

from .core import log
from .langutils import norm
from .dbcore import db, BaseModel

#########
# Enums #
#########

# used in Conflict and Failure records
class EntityOp(StrEnum):
    PARSE  = "parse"
    FIND   = "find"
    INSERT = "insert"
    UPDATE = "update"

##########
# Person #
##########

# Note: "comp" in this module means component (not composer)
NAME_COMPS         = ['title', 'first_name', 'middle_name', 'last_prefix', 'last_name', 'suffix']
SH_NAME_COMPS      = ['title', 'first_name', 'last_prefix', 'last_name', 'suffix']
VAR_NAME_COMPS     = ['title', 'middle_name', 'last_prefix', 'last_name', 'suffix']
ALT_NAME_COMPS     = ['last_name', 'suffix', 'title', 'first_name', 'middle_name', 'last_prefix']
ALT_SH_NAME_COMPS  = ['last_name', 'suffix', 'title', 'first_name', 'last_prefix']
ALT_VAR_NAME_COMPS = ['last_name', 'suffix', 'title', 'middle_name', 'last_prefix']

class Person(BaseModel):
    """Represents a person
    """
    name          = TextField()           # defaults to full_name
    disamb        = TextField(default='')
    alt_name      = TextField(null=True)  # typically, leading with last name

    # name components ("generally" normalized)
    title         = TextField(null=True)
    first_name    = TextField(null=True)  # also used for single-name persons
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
            (('alt_name',), False),
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

    @property
    def short_name(self) -> str:
        """ Short(er) construction of person's name, omitting middle_name.
        """
        if not self.first_name:
            return None
        comps = [getattr(self, x) for x in SH_NAME_COMPS if getattr(self, x)]
        # add comma before name suffix, if exists
        if self.suffix:
            assert len(comps) > 1
            comps[-2] += ','
        return ' '.join(comps)

    @property
    def var_name(self) -> str:
        """ Variant short(er) construction of person's name, omitting first_name.
        """
        if not self.middle_name:
            return None
        comps = [getattr(self, x) for x in VAR_NAME_COMPS if getattr(self, x)]
        # add comma before name suffix, if exists
        if self.suffix:
            assert len(comps) > 1
            comps[-2] += ','
        return ' '.join(comps)

    @property
    def alt_full_name(self) -> str:
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

    @property
    def alt_short_name(self) -> str:
        """ Short(er) construction of "alt" name, omitting middle_name.
        """
        if not self.first_name:
            return None
        comps = [getattr(self, x) for x in ALT_SH_NAME_COMPS if getattr(self, x)]
        # add comma after last name, or suffix (if exists)
        if self.last_name and len(comps) > 1:
            if not self.suffix:
                comps[0] += ','
            elif self.suffix and len(comps) > 2:
                comps[1] += ','
        return ' '.join(comps)

    @property
    def alt_var_name(self) -> str:
        """ Variant short(er) construction of "alt" name, omitting first_name.
        """
        if not self.middle_name:
            return None
        comps = [getattr(self, x) for x in ALT_VAR_NAME_COMPS if getattr(self, x)]
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
            alt_name = self.alt_full_name
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
    name_str      = TextField()           # raw name string (no fixup)
    name_str_norm = TextField(index=True)
    name_type     = TextField(null=True)
    source        = TextField(default='')
    source_date   = DateField(null=True)
    person        = ForeignKeyField(Person, null=True, backref='person_names')
    person_res    = TextField(null=True)  # person resolution mechanism (or process?)

    class Meta:
        indexes = (
            # REVISIT: should we add source_date (otherwise need to think about conflict
            # handling logic)!!!
            (('name_str', 'source'), True),
        )

    def save(self, *args, **kwargs):
        if 'name_str' in self._dirty:
            self.name_str_norm = norm(self.name_str)
        return super().save(*args, **kwargs)

########
# Work #
########

class Work(BaseModel):
    """Represents a composition
    """
    composer      = ForeignKeyField(Person, backref='works')
    name          = TextField()           # defaults to full_name
    disamb        = TextField(default='')
    alt_name      = TextField(null=True)

    # identifying components
    work_type     = TextField(null=True)  # or "genre"
    work_title    = TextField(null=True)  # includes ordinal, if any (e.g. "Symphony #4")
    work_subtitle = TextField(null=True)  # includes nicknames (e.g. "Eroica")
    work_key      = TextField(null=True)
    work_date     = TextField(null=True)
    catalog_no    = TextField(null=True)  # i.e. op., K., BWV, etc.

    # NOTE: canonical record is assumed to be normalized and authoritative
    is_canonical  = BooleanField(null=True)
    cnl_person_id = ForeignKeyField('self', null=True, backref='aliases')  # points to self, if canonical

    # reference info
    source        = TextField(null=True)
    source_date   = DateField(null=True)
    notes         = TextField(null=True)

    class Meta:
        indexes = (
            # REVISIT: should we add source_date (otherwise need to think about conflict
            # handling logic)!!!
            (('composer', 'name', 'disamb'), True),
        )

    @property
    def full_name(self) -> str:
        """ Construct full name from individual identifying components.
        """
        # TEMP: for now, full name is just work_title!!!
        return self.work_title

    def save(self, *args, **kwargs):
        if not self.name:
            self.name = self.full_name
        return super().save(*args, **kwargs)

############
# WorkMeta #
############

class WorkMeta(BaseModel):
    """Represents metainformation (additional field values) for a work (composition)
    """
    work          = ForeignKeyField(Work, backref='work_metas')
    key           = TextField()
    value         = TextField(null=True)
    source        = TextField(null=True)
    source_date   = DateField(null=True)

    class Meta:
        indexes = (
            # REVISIT: should we add source_date (otherwise need to think about conflict
            # handling logic)!!!
            (('work', 'key', 'source'), True),
        )

##############
# WorkName #
##############

class WorkName(BaseModel):
    """Represents the string used to identify a work (composition)
    """
    name_str      = TextField()           # raw name string (no fixup)
    source        = TextField(default='')
    source_date   = DateField(null=True)
    work          = ForeignKeyField(Work, null=True, backref='person_names')
    work_res      = TextField(null=True)  # work resolution mechanism (or process?)

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
    entity_name = TextField()
    entity_str  = TextField()                # raw name string (no fixup)
    entity_info = JSONField()                # entity attributes (plus ctx/metainfo)
    operation   = TextField()
    status      = TextField(default='open')  # 'open', 'in process, 'resolved', 'withdrawn'
    parent_id   = IntegerField(null=True)    # id of parent (resolved to) record

    class Meta:
        indexes = (
            (('entity_name', 'operation', 'entity_str'), False),
        )

###########
# Failure #
###########

class Failure(BaseModel):
    """Represents entity failures (e.g. parsing) to be resolved
    """
    entity_name = TextField()
    entity_str  = TextField()                # raw name string (no fixup)
    entity_info = JSONField()                # relevant ctx and metainfo (if any)
    operation   = TextField()
    status      = TextField(default='open')  # 'open', 'in process, 'resolved', 'withdrawn'
    parent_id   = IntegerField(null=True)    # id of parent (resolved to) record

    class Meta:
        indexes = (
            (('entity_name', 'operation', 'entity_str'), False),
        )

##########
# create #
##########

ALL_MODELS = [Person,
              PersonMeta,
              PersonName,
              Conflict,
              Failure,
              Work,
              WorkMeta,
              WorkName]

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
