# -*- coding: utf-8 -*-

"""Module for fetching and loading reference data from external sources.

Note that we are doing a lot of detailed source-specific processing, given empiricle
patterns in the data, since we are only using sources we believe to be well-curated and
generally consistent (though possibly quirky).  We are expecting that actual playlist data
will be far less consistent, so will do more fuzzy matching when processing those.
"""

import json
from collections.abc import Generator, Iterable
from typing import TextIO, NamedTuple
from datetime import date
from time import sleep
from glob import glob
from importlib import import_module
import os

import regex as re
import requests
from bs4 import BeautifulSoup
from peewee import IntegrityError

from .core import (cfg, log, DataFile, ConfigError, ImplementationError, DFLT_CHARSET,
                   DFLT_FETCH_INT, DFLT_HTML_PARSER)
from .langutils import norm
from .dbcore import now_str
from .schema import Person, PersonMeta, PersonName, Work, WorkMeta, WorkName, Conflict

REFDATA_DIR  = 'refdata'
BASE_CFG_KEY = 'refdata_base'
SOURCES_KEY  = 'refdata_sources'

base_cfg     = cfg.config(BASE_CFG_KEY)
sources      = cfg.config(SOURCES_KEY)

###############
# RefdataBase #
###############

class LoadCtx(NamedTuple):
    """
    """
    file:   str        # full pathname
    source:  str       # refdata source name (config key)
    source_date: date  # datestamp of the data

SegData = BeautifulSoup | dict

TOKEN_VARS = ['category', 'key', 'role']

class Refdata:
    """Abstract base class for a reference data source.
    """
    # base config parameters
    module_path:    str
    base_class:     str
    charset:        str            = DFLT_CHARSET
    fetch_interval: float          = DFLT_FETCH_INT
    html_parser:    str            = DFLT_HTML_PARSER
    http_headers:   dict[str, str] = {}

    # source config parameters
    name:           str
    full_name:      str
    subclass:       str
    dflt_keys:      str            = None
    categories:     dict[str, dict[str, str]]
    fetch_url:      str
    fetch_params:   dict[str, str] = {}
    fetch_format:   str
    data_format:    str

    @classmethod
    def new(cls, source_name: str, **kwargs) -> 'Refdata':
        """Return instantiated Refdata subclass instance.  Additional kwargs are shallow
        parameters overrides on top of base and source-specific configuration (supported,
        but not generally expected).
        """
        if source_name not in sources:
            raise RuntimeError(f"Refdata source '{source_name}' not known")
        source_cfg   = {'name': source_name} | base_cfg | sources[source_name] | kwargs
        class_name   = source_cfg.get('subclass')
        module_path  = source_cfg.get('module_path')
        if not class_name:
            raise ConfigError(f"'subclass' not specified for source '{source_name}'")
        if module_path:
            module = import_module(module_path)
            refdata_class = getattr(module, class_name)
        else:
            refdata_class = globals()[class_name]
        # ATTENTION: refdata_class and cls must be loaded from same module for this check
        # to work!
        if not issubclass(refdata_class, cls):
            raise ConfigError(f"'{refdata_class.__name__}' not subclass of '{cls.__name__}'")

        return refdata_class(**source_cfg)

    def __init__(self, **kwargs):
        """Note that caller is expected to pass in the appropriate parameters from the
        config file (plus any instantiation overrides).
        """
        for key, value in kwargs.items():
            setattr(self, key, value)
        pass  # TEMP: for debugging!!!

    def valid_key(self, key: str | None) -> bool:
        """A fetch key must be be a single lowercase letter (or ``None`` indicating all
        items).
        """
        if key is None:
            return True
        return len(key) == 1 and ord(key) in range(ord('a'), ord('z') + 1)

    def expand_keys(self, keys: str | None) -> Iterable[str | None]:
        """
        """
        if keys is None:
            return [keys]

        m = re.fullmatch(r'([a-z])-([a-z])', keys.lower())
        if m and m.group(1) <= m.group(2):
            return (chr(cc) for cc in range(ord(m.group(1)), ord(m.group(2)) + 1))
        else:
            return keys.split(',')

    def token_repl(self, s: str, **kwargs) -> str:
        """Replace tokens in ``s`` with values from kwargs or instance variables.
        """
        tokens = re.findall(r'(\<[\p{Lu}\d_]+\>)', s)
        for token in tokens:
            token_var = token[1:-1].lower()
            value = kwargs.get(token_var, getattr(self, token_var, None))
            if value is None:
                raise RuntimeError(f"Value not found for token {token} (in \"{s}\")")
            s = s.replace(token, str(value))
        return s

    def fetch_segs(self, category: str, keys: str = None,
                   dryrun: bool = False) -> Generator[tuple[str, str]]:
        """Generator for fetching individual segments for specified category and key(s).
        Yield value is a (key, data) tuple.
        """
        if category not in self.categories:
            raise RuntimeError(f"Category '{category}' not known for '{self.full_name}'")
        cat_cfg = self.categories[category]
        cat_params = cat_cfg.get('addl_params') or {}

        sess = requests.Session()
        keys = keys or self.dflt_keys
        keylist = self.expand_keys(keys)

        for i, key in enumerate(keylist):
            if not self.valid_key(key):
                raise RuntimeError(f"Invalid key '{key}' in \"{keys}\"")

            # note that both category and key are in TOKEN_VARS
            tokvals = {k: v for k, v in (cat_cfg | locals()).items() if k in TOKEN_VARS}
            url     = self.token_repl(self.fetch_url, **tokvals)
            params  = {k: self.token_repl(v, **tokvals)
                       for k, v in (self.fetch_params | cat_params).items()}

            log.info(f"Fetching from {url} (params: {params})")
            log.debug(f"HTTP headers: {self.http_headers}")

            if dryrun:
                req = requests.Request('GET', url, params=params, headers=self.http_headers)
                prep = req.prepare()
                log.info(f"Dryrun: GET '{prep.url}', headers: {prep.headers}")
                yield key, None
                continue

            if i > 0:
                sleep(self.fetch_interval)
            resp = sess.get(url, params=params, headers=self.http_headers)
            if not resp.ok:
                errmsg = f"GET '{resp.url}' returned status code {resp.status_code}"
                log.error(errmsg)
                raise RuntimeError(errmsg)
            yield key, resp.text

    def fetch(self, category: str, keys: str = None, force: bool = False, dryrun: bool = False,
              **kwargs) -> None:
        """
        """
        if kwargs:
            raise RuntimeError(f"Unexpected argument(s): {', '.join(kwargs.keys())}")

        for key, seg_data in self.fetch_segs(category, keys, dryrun=dryrun):
            if dryrun:
                assert seg_data is None
                continue

            seg_file = "%s:%s.%s" % (category, key, self.fetch_format)
            seg_dirs = [REFDATA_DIR, self.name, category]
            seg_path = DataFile(seg_file, seg_dirs)
            with open(seg_path, 'w') as f:
                nbytes = f.write(seg_data)
                log.info(f"{nbytes} bytes written to {seg_path}")

    def get_seg_data(self, fp: TextIO) -> SegData:
        """
        """
        if self.data_format == 'html':
            return BeautifulSoup(fp, self.html_parser)
        if self.data_format == 'json':
            return json.load(fp)

        raise ConfigError(f"Unknown data_format {self.data_format}")

    def read_segs(self, category: str, keys: str = None) -> Generator[tuple[str, SegData]]:
        """Generator for reading individual segment data files for specified category and
        key(s).  Yield value is a (key, data) tuple.
        """
        if category not in self.categories:
            raise RuntimeError(f"Category '{category}' not known for '{self.full_name}'")

        if keys is None:
            keys = self.dflt_keys
        keylist = self.expand_keys(keys)

        for key in keylist:
            if not self.valid_key(key):
                raise RuntimeError(f"Invalid key '{key}' in \"{keys}\"")
            if key is None:
                key = '*'

            seg_file = "%s:%s.%s" % (category, key, self.data_format)
            seg_dirs = [REFDATA_DIR, self.name, category]
            seg_glob = DataFile(seg_file, seg_dirs)
            for seg_path in glob(seg_glob):
                with open(seg_path) as fp:
                    seg_data = self.get_seg_data(fp)
                yield seg_path, seg_data

    def load(self, category: str, keys: str = None, force: bool = False, dryrun: bool = False,
             **kwargs) -> None:
        """
        """
        if kwargs:
            raise RuntimeError(f"Unexpected argument(s): {', '.join(kwargs.keys())}")
        cat_cfg = self.categories[category]
        loader = cat_cfg.get('loader')
        if not loader:
            raise ConfigError(f"Category {category} does not have a 'loader' attribute")
        load_func = getattr(self, loader)

        for file, data in self.read_segs(category, keys):
            file_mtime = os.stat(file).st_mtime
            ctx = LoadCtx(file, self.name, date.fromtimestamp(file_mtime))
            ins, upd, skip = load_func(ctx, data, dryrun)
            log.info(f"Load from {file}: {ins} inserted, {upd} updated, {skip} skipped")

###############
# RefdataCLMU #
###############

class RefdataCLMU(Refdata):
    """
    """
    def valid_key(self, key: int | str | None) -> bool:
        """A fetch key must be be a valid page number (or ``None``, indicating all pages).
        """
        if key is None:
            return True
        return isinstance(key, int) or re.fullmatch(r'\d+', key)

    def expand_keys(self, keys: str | int | None) -> Iterable[str | int | None]:
        """
        """
        if keys is None or isinstance(keys, int):
            return [keys]

        m = re.fullmatch(r'(\d+)-(\d+)', keys)
        if m and int(m.group(1)) <= int(m.group(2)):
            return range(int(m.group(1)), int(m.group(2)) + 1)
        else:
            return keys.split(',')

    def parse_comp_name(self, comp_name: str) -> Person | None:
        """
        """
        TITLES           = ['Sir']
        TITLES_CI        = []
        LAST_PREFIXES    = ['de', 'da', 'del', 'van', 'von', 'van der', 'of', 'di']
        LAST_PREFIXES_CI = ['de', 'da', 'del', 'van', 'von', 'van der']
        SUFFIXES         = ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X',
                            'Jr.', 'Sr.', 'the Elder', 'the Younger', 'El Viejo', 'El Joven']
        SUFFIXES_CI      = ['jr.', 'sr.', 'the elder', 'the younger', 'el viejo', 'el joven',
                            'le père', 'le fils', 'père', 'fils']

        person = Person()
        pieces = comp_name.split(', ')

        # only look for last name prefix if 3 or more pieces, to guard against the case
        # where last name is a case-insensiive match (e.g. "Van, Jeffrey")
        if len(pieces) >= 3:
            if pieces[0] in LAST_PREFIXES or pieces[0].lower() in LAST_PREFIXES_CI:
                assert not person.last_prefix
                person.last_prefix = pieces.pop(0)

        # name suffix can come from the end, or just after the last name
        if len(pieces) > 1:
            if pieces[-1] in SUFFIXES or pieces[-1].lower() in SUFFIXES_CI:
                assert not person.suffix
                person.suffix = pieces.pop(-1)
            if pieces[1] in SUFFIXES or pieces[1].lower() in SUFFIXES_CI:
                assert not person.suffix
                person.suffix = pieces.pop(1)

        if len(pieces) >= 3:
            # don't know how to parse from here, so will have to be manually rectified;
            # any parsing done above is discarded
            return None

        if len(pieces) == 1:
            # REVISIT: may want to try parsing single piece on whitespace (e.g. "name
            # (alias)")!
            if person.last_prefix:
                assert not person.last_name
                person.last_name = pieces.pop(0)
            else:
                assert not person.first_name
                person.first_name = pieces.pop(0)
            return person

        assert len(pieces) == 2
        assert len(pieces[0]) > 0
        assert len(pieces[1]) > 0
        last_pieces = pieces[0].split(' ')
        first_pieces = pieces[1].split(' ')

        # title (if present) is usually at the leading edge of first_pieces, but may also
        # be represented as the entirety of first_pieces (in which case we will do our
        # best shot at parsing out the first name from last_pieces)
        if first_pieces[0] in TITLES:
            assert not person.title
            person.title = first_pieces.pop(0)
            if not first_pieces:
                first_pieces.append(last_pieces.pop(0))

        # look for last name prefix in the leading portion of last_pieces or the trailing
        # portion of first_pieces; this is very not pretty, but we hardwire the ability to
        # look for one and two word strings
        if last_pieces[0] in LAST_PREFIXES:
            assert not person.last_prefix
            person.last_prefix = last_pieces.pop(0)
        elif ' '.join(last_pieces[:2]) in LAST_PREFIXES:
            assert not person.last_prefix
            person.last_prefix = last_pieces.pop(0) + ' ' + last_pieces.pop(0)

        if first_pieces[-1] in LAST_PREFIXES:
            assert not person.last_prefix
            person.last_prefix = first_pieces.pop(-1)
        elif ' '.join(first_pieces[-2:]) in LAST_PREFIXES:
            assert not person.last_prefix
            person.last_prefix = first_pieces.pop(-2) + ' ' + first_pieces.pop(-1)

        # there is also the case where a last_prefix/last_name sequence is preceded by a
        # comma (e.g. "Hildegard, of Bingen"), more like a suffix representation; for
        # consistency, we will swap the parts and parse as above

        if first_pieces[0] in LAST_PREFIXES:
            last_pieces, first_pieces = first_pieces, last_pieces
            assert not person.last_prefix
            person.last_prefix = last_pieces.pop(0)
        elif ' '.join(first_pieces[:2]) in LAST_PREFIXES:
            last_pieces, first_pieces = first_pieces, last_pieces
            assert not person.last_prefix
            person.last_prefix = last_pieces.pop(0) + ' ' + last_pieces.pop(0)

        # first handle cases of only one name field remaining (either last or first)
        assert last_pieces or first_pieces
        if not last_pieces:
            if person.last_prefix:
                # treat first_pieces as last name
                assert not person.last_name
                person.last_name = ' '.join(first_pieces)
            else:
                assert not person.first_name
                person.first_name = ' '.join(first_pieces)
            return person
        if not first_pieces:
            assert not person.last_name
            person.last_name = ' '.join(last_pieces)
            return person

        # now handle cases where we have to decide where stuff goes
        assert last_pieces and first_pieces
        if len(first_pieces) > 1:
            # ATTENTION: disabling the following manipulation for now (clever, but doing
            # more harm than good)!!!
            r'''
            # coelesce leading initials in first_pieces (e.g. "J. S." -> "J.S.")
            if m := re.fullmatch(r'(\p{Lu}\.( \p{Lu}\.)+)(.*)', ' '.join(first_pieces)):
                assert not person.first_name
                person.first_name = m.group(1).replace(' ', '')
                if m.group(3):
                    # leftovers form the middle name
                    assert not person.middle_name
                    person.middle_name = m.group(3).strip()
            else:
            '''
            if True:  # TEMP: to keep proper indent level for this block
                assert not person.first_name
                assert not person.middle_name
                person.first_name = first_pieces.pop(0)
                person.middle_name = ' '.join(first_pieces)
        else:
            assert not person.first_name
            person.first_name = first_pieces.pop(0)

        assert not person.last_name
        person.last_name = ' '.join(last_pieces)
        return person

    def load_composer(self, ctx: LoadCtx, data: SegData,
                      dryrun: bool = False) -> tuple[int, int, int]:
        """Return tuple of record counts: [inserted, updated, skipped].
        """
        assert isinstance(data, BeautifulSoup)
        ins  = 0
        upd  = 0
        skip = 0
        soup = data
        content = soup.select_one("div.view-content")
        for tr in content.select("tbody tr"):
            comp = tr.select_one("td.views-field-name").string.strip()
            link = tr.select_one("td.views-field-count").a['href']
            comp_name = None
            addl_info = None
            alt_comp  = None
            meta      = {}

            # Rule 1 - match any of the following line endings (will be added to person
            # metainfo as "floruit"):
            #   ", fl. 1971"
            #   ", fl. 1430-1439"
            #   " fl. 1675"
            #   " fl. 1698-1698"
            #
            # Note that we are not enforcing exactly 4 digits per year, to allow for
            # variability
            rule1 = r'(.+?)(,? fl\. ([0-9-]+(\-[0-9]+)?))'

            # Rule 2 - match any of the following line endings (will be added to person
            # metainfo as "dates"):
            #   ", 1971-"
            #   ", 1430-1439"
            #   " 1975-"
            #   " 1698-1698"
            #
            # Same as above regarding date formatting (though this rule only recognizes
            # dates as years)
            rule2 = r'(.+?)(,? (([0-9-]+)\-([0-9]+)?))'

            if m := re.fullmatch(rule1, comp):
                comp_name = m.group(1)
                addl_info = m.group(2)
                meta['floruit'] = m.group(3)
            elif m := re.fullmatch(rule2, comp):
                comp_name = m.group(1)
                addl_info = m.group(2)
                meta['dates'] = m.group(3)
                meta['born'] = m.group(4)
                if m.group(5):
                    meta['died'] = m.group(5)
            else:
                comp_name = comp

            # source-specific processing for creating an alternate version of comp if
            # addl_info is present (basically, swap first two comma-delimited fields,
            # omitting the intervening comma)
            if addl_info:
                pieces = comp_name.split(', ', 2)
                if len(pieces) == 2:
                    alt_comp = f"{pieces[1]} {pieces[0]}{addl_info}"
                elif len(pieces) > 2:
                    alt_comp = f"{pieces[1]} {pieces[0]}, {pieces[2]}{addl_info}"
                else:
                    assert len(pieces) == 1
                    # also take care of case where one-part name has no comma separator
                    # for addl_info
                    if addl_info[0] == ' ':
                        alt_comp = f"{pieces[0]},{addl_info}"

            # structural fixup for comp_name: fix embedded and trailing commas; try and be
            # as specific as possible initially (can broaden as needed, based on anomalies)
            comp_name = re.sub(r'(\pL)\,(\pL)', r'\1, \2', comp_name)
            if comp_name[-1] == ',':
                comp_name = comp_name.rstrip(',')

            if link:
                meta['clmu_link'] = link

            comp_person = self.parse_comp_name(comp_name)
            if dryrun:
                full_name = comp_person.full_name if comp_person else '[UNPARSED]'
                print(f"{comp_name} => {full_name}")
                #print(comp_name, meta)
                continue

            if not comp_person:
                log.info(f"Could not parse comp_name '{comp_name}'")
                skip += 1
                continue
            new_comp = None
            now_ts = now_str()  # use same timestamp for everything here
            try:
                if addl_info:
                    comp_person.disamb = addl_info.lstrip(', ')
                comp_person.is_composer = True
                comp_person.source = ctx.source
                comp_person.source_date = str(ctx.source_date)
                comp_person.created_at = now_ts
                comp_person.updated_at = now_ts
                comp_person.save()
                new_comp = True
                ins += 1
            except IntegrityError as e:
                person_data = dict(comp_person.__data__)
                person_data['meta'] = meta
                log.info(f"Conflict saving Person: {person_data}")
                Conflict.create(entity_name=comp_person.__class__.__name__,
                                entity_def=person_data,
                                created_at=now_ts,
                                updated_at=now_ts)

                comp_person = Person.get(Person.name == comp_person.name,
                                         Person.disamb == comp_person.disamb)
                # note that we fall through here so that we can (possibly) add new
                # person_names or metadata (either of which would constitute an update)
                new_comp = False
                # FIX: we don't know if this is really an update yet, but for now just do
                # this to distinguish from unparseable/skip!!!
                upd += 1

            try:
                # REVISIT: there needs to be a process for disambiguating names whenever
                # duplicates are added to (or detected in) Person!!!
                PersonName.create(name_str=comp,
                                  source=ctx.source,
                                  source_date=str(ctx.source_date),
                                  person=comp_person,
                                  person_res='load_composer',
                                  created_at=now_ts,
                                  updated_at=now_ts)

                if alt_comp:
                    PersonName.create(name_str=alt_comp,
                                      source=ctx.source,
                                      source_date=str(ctx.source_date),
                                      person=comp_person,
                                      person_res='load_composer',
                                      created_at=now_ts,
                                      updated_at=now_ts)
            except IntegrityError as e:
                # this presumes that only the first insert would fail this way
                log.info(f"Duplicate PersonName '{comp}' (0)")

            if not new_comp:
                continue

            if comp_name != comp:
                try:
                    PersonName.create(name_str=comp_name,
                                      source=ctx.source,
                                      source_date=str(ctx.source_date),
                                      person=comp_person,
                                      person_res='load_composer',
                                      created_at=now_ts,
                                      updated_at=now_ts)
                except IntegrityError as e:
                    log.info(f"Duplicate PersonName '{comp_name}' (1)")

            meta_items = []
            for k, v in meta.items():
                meta_items.append({'person':      comp_person.id,
                                   'key':         k,
                                   'value':       v,
                                   'source':      ctx.source,
                                   'source_date': str(ctx.source_date),
                                   'created_at':  now_ts,
                                   'updated_at':  now_ts})
            PersonMeta.insert_many(meta_items).execute()

            for i, name_str in enumerate([comp_person.name, comp_person.alt_name]):
                if not name_str or name_str == comp_name:
                    continue
                try:
                    PersonName.create(name_str=name_str,
                                      source=ctx.source,
                                      source_date=str(ctx.source_date),
                                      person=comp_person,
                                      person_res='load_composer',
                                      created_at=now_ts,
                                      updated_at=now_ts)
                except IntegrityError as e:
                    log.info(f"Duplicate PersonName '{name_str}' ({i+2})")
                    pass

        return ins, upd, skip

    def find_composer(self, ctx: LoadCtx, comp_name: str,
                      id_only: bool = False) -> Person | int | None:
        """
        """
        query = (PersonName
                 .select()
                 .join(Person)
                 .where(PersonName.name_str == comp_name,
                        PersonName.source == ctx.source))
        pnames = list(query.execute())
        if not pnames:
            # try normalized name match (combine with above?)
            query = (PersonName
                     .select()
                     .join(Person)
                     .where(PersonName.name_str_norm == norm(comp_name),
                            PersonName.source == ctx.source))
            pnames = list(query.execute())
            if not pnames:
                return None
            # TODO (maybe): add case-sensitive match to PersonName???
        assert len(pnames) == 1
        comp = pnames[0].person
        if not comp.is_composer:
            log.info(f"Setting is_composer flag for Person ID {comp.id} ({comp.name}) ")
            comp.is_composer = True
            comp.save()
        return comp.id if id_only else comp

    def load_work(self, ctx: LoadCtx, data: SegData, dryrun: bool = False) -> tuple[int, int, int]:
        """Return tuple of record counts: [inserted, updated, skipped].
        """
        assert isinstance(data, BeautifulSoup)
        ins  = 0
        upd  = 0
        skip = 0
        soup = data
        content = soup.select_one("div.view-content")
        for i, item_div in enumerate(content.select("div.lazr-browse-composition-item")):
            title_div   = item_div.select_one("div.lazr-browse-composition-title")
            compsr_div  = item_div.select_one("div.lazr-browse-composition-composer")
            perfs_ul    = item_div.select_one("ul.lazr-browse-composition-performances")
            title_span  = perfs_ul.li.find('span', attrs={'data-field': "real_title"})

            item_title  = item_div['title']
            genre       = item_div['genre']
            composed    = item_div['composed']
            title       = title_div.span.string.strip()
            if title != item_title:
                log.info(f"{title} != {item_title} ({ctx.file}:{i})")
            if not compsr_div:
                log.info(f"{item_title} has no composer div ({ctx.file}:{i})")
                continue
            compsr_name = compsr_div.span.string.strip()
            real_title  = title_span.string.strip()

            meta = {}
            meta['short_title'] = title

            # look for a quick match without having to parse
            composer = self.find_composer(ctx, compsr_name)
            if not composer:
                # TODO: have to do more work in processing composer string (possibly
                # adding person and/or records)!!!
                log.info(f"Composer name '{compsr_name}' not found, skipping work {real_title}")
                continue
            #work = self.parse_work_name(real_title)
            if dryrun:
                print(f"{composer.name}: {real_title}")
                #print(f"{real_title} => {work.name}")
                #print(comp_name, meta)
                continue

            work = Work()
            work.composer = composer
            if not work:
                skip += 1
                continue
            new_work = None
            now_ts = now_str()  # use same timestamp for everything here
            try:
                work.work_type   = genre
                work.work_title  = real_title
                work.work_date   = composed
                work.source      = ctx.source
                work.source_date = str(ctx.source_date)
                work.created_at  = now_ts
                work.updated_at  = now_ts
                work.save()
                new_work = True
                ins += 1
            except IntegrityError as e:
                work_data = dict(work.__data__)
                work_data['meta'] = meta
                log.info(f"Conflict saving Work: {work_data}")
                Conflict.create(entity_name=work.__class__.__name__,
                                entity_def=work_data,
                                created_at=now_ts,
                                updated_at=now_ts)

                work = Work.get(Work.composer == work.composer,
                                Work.name == work.name,
                                Work.disamb == work.disamb)
                # FIX: not really updating, but separate this case from unparseable!!!
                # note that we fall through here so that we can (possibly) add new
                # work_names
                new_work = False
                upd += 1

        return ins, upd, skip

################
# RefdataIMSLP #
################

class RefdataIMSLP(Refdata):
    """
    """
    def fetch(self, category: str, keys: str = None, force: bool = False, dryrun: bool = False,
              **kwargs) -> None:
        """
        """
        raise ImplementationError(f"fetch() not yet implemented for {self.name}")

#################
# RefdataPresto #
#################

class RefdataPresto(Refdata):
    """
    """
    pass

################
# RefdataArkiv #
################

class RefdataArkiv(Refdata):
    """
    """
    def fetch(self, category: str, keys: str = None, force: bool = False, dryrun: bool = False,
              **kwargs) -> None:
        """
        """
        raise ImplementationError(f"fetch() not yet implemented for {self.name}")

###################
# RefdataOpenOpus #
###################

class RefdataOpenOpus(Refdata):
    """
    """
    def fetch(self, category: str, keys: str = None, force: bool = False, dryrun: bool = False,
              **kwargs) -> None:
        """
        """
        raise ImplementationError(f"fetch() not yet implemented for {self.name}")

########
# main #
########

import sys

from ckautils import parse_argv

ACTIONS = ['fetch', 'load']

def main() -> int:
    """Usage::

      $ python -m refdata <source> <action> <category> [<arg>=<val> [...]]

    Actions (and associated arguments):

      - fetch keys=<key(s)> force=<bool> dryrun=<bool>

      - load keys=<key(s)> force=<bool> dryrun=<bool>

    where:

      - <key(s)> is either a single key (typically the initial letter of a name, but can
        be a results page number for some sources), a comma-separated list of keys, or a
        key range (designated by start-end); complete set of data within the category is
        assumed if ``keys`` is omitted

      - ``force`` (optional, default false) indicates that existing entries should be
        overwritten

      - ``dryrun`` (optional, default false) indicates that writes to files or database
         are not performed (would-be actions are printed to stdout instead)

    """
    if len(sys.argv) < 4:
        print(f"Insufficient arguments specified", file=sys.stderr)
        return -1
    source = sys.argv[1]
    source_cfg = sources.get(source)
    if not source_cfg:
        print(f"Source '{source}' not known", file=sys.stderr)
        return -1
    action = sys.argv[2]
    if action not in ACTIONS:
        print(f"Action '{action}' not known", file=sys.stderr)
        return -1
    category = sys.argv[3]
    if category not in source_cfg['categories']:
        print(f"Category '{category}' not known", file=sys.stderr)
        return -1

    args, kwargs = parse_argv(sys.argv[4:])
    if args:
        print(f"Unexpected argument(s): {', '.join(args)}", file=sys.stderr)
        return -1

    refdata = Refdata.new(source)
    refdata_func = getattr(refdata, action)

    # note that kwargs are validated by the action; return value is ignored (exceptions
    # should be raised for errors)
    refdata_func(category, **kwargs)
    return 0

if __name__ == '__main__':
    sys.exit(main())
