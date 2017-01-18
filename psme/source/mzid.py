from pyteomics.mzid import read

from psme.peptide import Peptide
from psme.psm import Psm
from psme.peak_list import ScanReference
import sys, re


class MzIdLoader(object):

    def __init__(self, settings, scan_source_manager):
        self.settings = settings
        self.scan_source_manager = scan_source_manager

    def load(self, f, source_statistic_names):
        dbg_limit = 10
        counter = 0
        title_patterns = [r'scan=(\d+)',r'^.*?(?:[.][0]*(\d+)){2}[.]\d+$']
        psms = []
        for identification_result in read(f, retrieve_refs=True):
            scan_id = identification_result['spectrumID']
            scan_num = None
            spectrum_title = identification_result.get('spectrum title',None)
            if spectrum_title:
                for title_pat in title_patterns:
                    m = re.search(title_pat,spectrum_title)
                    if m:
                        scan_num = int(m.groups()[0])
                        break
            scan_source = self.scan_source_manager.match_by_name(identification_result['name'])
            if counter < dbg_limit:
                print >> sys.stderr, "MzIdLoader %s\n" % identification_result
            ## source, index=None, number=None, id=None, base_peak_mz=None)
            scan_reference = ScanReference(id=scan_id, number=scan_num, source=scan_source)
            if counter < dbg_limit:
                print >> sys.stderr, "MzIdLoader scan %s\t%s\t%s\t%s\t%s\n" % ( scan_reference.id, scan_reference.index, scan_reference.number, scan_reference.base_peak_mz, scan_reference.source.name)
            counter += 1
            for identification_item in identification_result['SpectrumIdentificationItem']:
                psm = self._identification_to_psm(identification_item, scan_reference, source_statistic_names)
                psms.append(psm)
        return psms

    def _identification_to_psm(self, identification_item, scan_reference, source_statistic_names):
        sequence = identification_item['PeptideSequence']
        mzid_modifications = identification_item.get('Modification', [])
        mods = self._convert_modifications(mzid_modifications)
        peptide = Peptide(sequence=sequence, modifications=mods)
        source_statistics = {}
        for source_statistic_name in source_statistic_names:
            source_statistic = identification_item.get(source_statistic_name, None)
            source_statistics[source_statistic_name] = source_statistic
        for key in identification_item.keys():
            if key == 'PeptideEvidenceRef':
                source_statistic = identification_item.get(key, None)
                source_statistics[key] = source_statistic
            elif key not in source_statistic_names:
                source_statistic = identification_item.get(key, None)
                source_statistics[key] = source_statistic
        psm = Psm(scan_reference=scan_reference,
                  peptide=peptide,
                  source_statistics=source_statistics)
        return psm

    def _convert_modifications(self, modifications):
        to_mod = lambda modification: {"position": self._get_position(modification),
                                       "mod_mass": modification['monoisotopicMassDelta']}
        return map(to_mod, modifications)

    def _get_position(self, modification):
        return int(modification['location']) - 1
