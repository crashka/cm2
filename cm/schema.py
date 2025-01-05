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

class Person(BaseModel):
    """Represents a person
    """
    name          = TextField(null=True)  # display value
    disamb        = TextField(null=True, default='')
    raw_name      = TextField(null=True)  # REVISIT: do we really need this???
    full_name     = TextField(null=True)  # assembled from normalized name components
    alt_name      = TextField(null=True)  # PLACEHOLDER: not sure what this means yet!!!

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

##############
# PersonName #
##############

class PersonName(BaseModel):
    """Represents a person name
    """
    name          = TextField(unique=True)
    addl_info     = TextField(null=True)
    source        = TextField(null=True)
    source_date   = DateField(null=True)
    sources       = JSONField(default=[])  # JSON array of (source, source_date) tuples
    person        = ForeignKeyField(Person, null=True, backref='person_names')
    person_res    = TextField(null=True)

##########
# create #
##########

ALL_MODELS = [Person,
              PersonName]

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
# Main #
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
        (default)

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
