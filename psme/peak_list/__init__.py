from pyteomics.mzml import read as mzml_read
from re import search
from os.path import basename
from numpy import argmax
import sys


class ScanReference(object):

    def __init__(self, source, index=None, number=None, id=None, base_peak_mz=None):
        self.index = index
        self.number = number
        self.id = id
        self.source = source
        self.base_peak_mz = base_peak_mz

    def matches_scan_from_same_source(self, scan):
        SEARCH_ORDER = ['id', 'index', 'number']
        for field in SEARCH_ORDER:
            value = getattr(self, field, None)
            if value and value == getattr(scan, field):
                return True
        return False


class ScanSourceManager(object):

    def __init__(self, settings):
        sources_enum = enumerate(settings.get("peak_lists"))
        self.scan_sources = [ScanSource(index, source_options) for index, source_options in sources_enum]

    def match_by_name(self, name):
        if len(self.scan_sources) == 1:
            return self.scan_sources[0]
        else:
            for scan_source in self.scan_sources:
                if scan_source.name == name:
                    return scan_source
            for scan_source in self.scan_sources:
                if scan_source.alias and scan_source.alias == name:
                    return scan_source
            raise Exception("Could not find scan source matching name [%s]" % name)

    def match_by_index(self, index):
        if len(self.scan_sources) == 1:
            return self.scan_sources[0]
        else:
            return self.scan_sources[index]

    def get_scans(self):
        for scan_source in self.scan_sources:
            for scan in scan_source.get_scans():
                yield scan


class ScanSource(object):

    def __init__(self, index, source_options_or_path):
        self.index = index
        if isinstance(source_options_or_path, dict):
            source_options = source_options_or_path
            self.path = source_options.get("path")
            # Name and encoded id are optional.
            self.name = source_options.get("name", None)
            self.alias = source_options.get("alias", None)
            self.encoded_id = source_options.get("encoded_id", None)
        else:
            path = source_options_or_path
            self.path = path
            self.name = None
            self.alias = None
            self.encoded_id = None
        self.filename = basename(self.path)
        if not self.name:
            self.name = self.filename
        print >> sys.stderr, "ScanSource source: %s name: %s alias: %s" % (self.path,self.name if self.name else  'none',self.alias if self.alias else 'none')

    def get_scans(self):
        #scan_sources = __load_scan_sources(settings)
        #for scan_source in scan_sources:
        index = 0
        # For now just assume MZML.
        for spectrum in mzml_read(open(self.path, 'r')):
            yield mzml_spectrum_to_scan(spectrum, self, index)
            index += 1


class Scan(object):

    def __init__(self, source, index, intensity_array, mz_array, number=None, id=None, base_peak_mz=None):
        self.source = source
        self.index = index
        self.intensity_array = intensity_array
        self.mz_array = mz_array
        self.base_peak_mz = base_peak_mz
        self._number = number
        self._id = id

    @property
    def number(self):
        return self._number or (self.index + 1)

    @property
    def id(self):
        return self._id or self.index


def mzml_spectrum_to_scan(spectrum, source, index):
    intensity_array = spectrum['intensity array']
    mz_array = spectrum['m/z array']
    try:
        base_peak_mz = spectrum['base peak m/z']
    except KeyError:
        base_peak_mz = mz_array[argmax(intensity_array)]
    id = spectrum['id']
    number = __mzml_id_to_number(id)
    return Scan(source=source, index=index, intensity_array=intensity_array, mz_array=mz_array, id=id, number=number, base_peak_mz=base_peak_mz)


def __mzml_id_to_number(scan_id):
    """

    >>> __mzml_id_to_number('test_id')
    >>> __mzml_id_to_number('controllerType=0 controllerNumber=1 scan=5')
    5
    """
    scan_match = search(r'scan=(\d+)', scan_id)
    number = None
    if scan_match:
        number = int(scan_match.group(1))
    return number
