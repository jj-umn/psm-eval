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

    def __init__(self, source, index, intensity_array, mz_array, number=None, id=None, ms_level=None, 
                 precursor_scan_num=None, precursor_charge=None, precursor_mz=None, precursor_intensity=None,
                 base_peak_mz=None, base_peak_intensity=None, total_ion_current=None, retention_time=None):
        self.source = source
        self.index = index
        self.ms_level = ms_level
        self.precursor_scan_num = precursor_scan_num
        self.precursor_charge = precursor_charge
        self.precursor_mz = precursor_mz
        self.precursor_intensity = precursor_intensity
        self.intensity_array = intensity_array
        self.mz_array = mz_array
        self.base_peak_mz = base_peak_mz
        self.base_peak_intensity = base_peak_intensity
        self.total_ion_current = total_ion_current
        self.retention_time = retention_time
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
    base_peak_intensity = spectrum['base peak intensity'] if 'base peak intensity' in spectrum else None
    ms_level = spectrum['ms level'] if 'ms level' in spectrum else None
    precursor_scan_num = None
    precursor_charge = None
    precursor_mz = None
    precursor_intensity = None
    if ms_level > 1 and 'precursorList' in spectrum and spectrum['precursorList']['count'] > 0:
        if 'spectrumRef' in spectrum['precursorList']['precursor'][0]:
            precursor_scan_num = __mzml_id_to_number(spectrum['precursorList']['precursor'][0]['spectrumRef'])
        if 'selectedIonList' in spectrum['precursorList']['precursor'][0]:
            precursor = spectrum['precursorList']['precursor'][0]['selectedIonList']['selectedIon'][0]
            precursor_charge = precursor['charge state'] if 'charge state' in precursor else None
            precursor_mz = precursor['selected ion m/z'] if 'selected ion m/z' in precursor else None
            precursor_intensity = precursor['peak intensity'] if 'peak intensity' in precursor else None
    total_ion_current = spectrum['total ion current'] if 'total ion current' in spectrum else None
    try:
        retention_time = spectrum['scan'][0]['scan start time']* 60 
    except:
        retention_time = None
    id = spectrum['id']
    number = __mzml_id_to_number(id)
    return Scan(source=source, index=index, intensity_array=intensity_array, mz_array=mz_array, id=id, number=number, 
                base_peak_mz=base_peak_mz, base_peak_intensity=base_peak_intensity, ms_level=ms_level, 
                precursor_scan_num=precursor_scan_num, 
                precursor_charge=precursor_charge, precursor_mz=precursor_mz, precursor_intensity=precursor_intensity,
                total_ion_current=total_ion_current, retention_time=retention_time)


def __mzml_id_to_number(scan_id):
    """
    >>> __mzml_id_to_number('test_id')
    >>> __mzml_id_to_number('controllerType=0 controllerNumber=1 scan=5')
    >>> __mzml_id_to_number('proj_3386_082312_11941_iTQ_sample_13_25.0005.0005.2
')
    5
    """
    patterns = [r'scan=(\d+)',r'^.*?(?:[.][0]*(\d+)){2}[.]\d+$']
    number = None
    for pat in patterns:
        scan_match = search(pat, scan_id)
        if scan_match:
            number = int(scan_match.group(1))
            break;
    return number
