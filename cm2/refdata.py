# -*- coding: utf-8 -*-

"""Module for fetching and loading reference data from external sources.
"""

import regex as re
from collections.abc import Generator, Iterable
from importlib import import_module
from time import sleep

import requests

from .core import cfg, log, DataFile, ConfigError, ImplementationError

REFDATA_DIR  = 'refdata'
BASE_CFG_KEY = 'refdata_base'
SOURCES_KEY  = 'refdata_sources'

DFLT_FETCH_INTERVAL = 1.0

base_cfg     = cfg.config(BASE_CFG_KEY)
sources      = cfg.config(SOURCES_KEY)

###############
# RefdataBase #
###############

TOKEN_VARS = ['category', 'key', 'role']

class Refdata:
    """Abstract base class for a reference data source.
    """
    # base config parameters
    module_path:    str
    base_class:     str
    charset:        str
    fetch_interval: float = DFLT_FETCH_INTERVAL
    html_parser:    str
    http_headers:   dict[str, str]

    # source config parameters
    name:           str
    full_name:      str
    subclass:       str
    dflt_keys:      str = None
    categories:     dict[str, dict[str, str]]
    fetch_url:      str
    fetch_params:   dict[str, str] = {}
    format:         str

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
        # ATTENTION: `refdata_class` and `cls` must be loaded from same module for this
        # check to work!
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
            return [None]

        m = re.fullmatch(r'([a-z])-([a-z])', keys.lower())
        if m and m.group(1) <= m.group(2):
            return (chr(cc) for cc in range(ord(m.group(1)), ord(m.group(2)) + 1))
        else:
            return keys.split(',')

    def token_repl(self, s: str, **kwargs) -> str:
        """Replace tokens in ``s`` with values from instance variables or kwargs.
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
            if i > 0:
                sleep(self.fetch_interval)

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

            seg_file = "%s:%s.%s" % (category, key, self.format)
            seg_dirs = [REFDATA_DIR, self.name, category]
            seg_path = DataFile(seg_file, seg_dirs)
            with open(seg_path, 'w') as f:
                nbytes = f.write(seg_data)
                log.info(f"{nbytes} bytes written to {seg_path}")

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

#################
# RefdataPresto #
#################

class RefdataPresto(Refdata):
    """
    """
    pass

###############
# RefdataCLMU #
###############

class RefdataCLMU(Refdata):
    """
    """
    def valid_key(self, key: int | str | None) -> bool:
        """A fetch key must be be a valid page number (or ``None``, indicating all pages).
        """
        assert key is not None  # see TEMP in expand_keys(), below!
        return isinstance(key, int) or re.fullmatch(r'\d+', key)

    def expand_keys(self, keys: str | None) -> Iterable[str | int]:
        """
        """
        if keys is None:
            # TEMP: `None` not currently supported (LATER, we will use this to mean
            # following the 'pager-next' href)!!!
            raise ImplementationError(f"Fetching all not yet supported for {self.name}")

        m = re.fullmatch(r'(\d+)-(\d+)', keys)
        if m and int(m.group(1)) <= int(m.group(2)):
            return range(int(m.group(1)), int(m.group(2)) + 1)
        else:
            return keys.split(',')

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

      - fetch keys=<key(s)> force=<bool>
      - load keys=<key(s)> force=<bool>

    where:

      - <key(s)> is either a single letter (indicating the initial letter of the name), a
        comma-separated list of letters, or a letter range (designated by start-end); all
        entries are assumed if `keys` is omitted
      - `force` (optional) is false by default

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

    refdata_func(category, **kwargs)  # any return (no exception) is considered success
    return 0

if __name__ == '__main__':
    sys.exit(main())
