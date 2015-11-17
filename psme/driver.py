from .source import load_psms
from .psm import PsmManager
from .column import build_column_providers
from .column import get_column_title
from .peak_list import ScanSourceManager
from .output import OutputFormatter
from .sqlitedb import sqldb
import sys


def evaluate(settings, output_formatter=OutputFormatter):
    """
    Collect statistics about each PSM and write out to file
    based on output_formatter.
    """
    collected_statistics = collect_statistics(settings)
    with output_formatter(settings) as output:
        for row in collected_statistics:
            output.write_row(row)


def collect_statistics(settings):
    """
    Build data structure describing statistics for each PSM.
    """
    columns = build_column_providers(settings)
    collected_names = [get_column_title(column) for column in columns]
    print >> sys.stdout, "collected_names: %s" % collected_names
    source_statistic_names = __find_referenced_source_statistics(columns)
    scan_source_manager = ScanSourceManager(settings)
    psms = load_psms(settings, scan_source_manager, source_statistic_names)
    psm_manager = PsmManager(psms)
    collected_statistics = __collect_statistics(scan_source_manager, psm_manager, columns)
    sqldb(settings,scan_source_manager,psm_manager,collected_names,collected_statistics,source_statistic_names)
    return collected_statistics

def __find_referenced_source_statistics(columns):
    source_statistic_names = set()
    for column in columns:
        source_statistic_name = getattr(column, 'source_statistic_name', None)
        if source_statistic_name:
            source_statistic_names.add(source_statistic_name)
    return list(source_statistic_names)


def __collect_statistics(scan_source_manager, psm_manager, statistics):
    psms_statistics = []
    for scan in scan_source_manager.get_scans():
        print >> sys.stderr, "__collect_statistics scan: %s" % scan.number
        for psm in psm_manager.psms_for_scan(scan):
            print >> sys.stderr, "__collect_statistics psm: %s" % psm.peptide.sequence
            psm_statistics = __psm_statistics(scan, psm, statistics)
            psms_statistics.append(psm_statistics)
    return psms_statistics


def __psm_statistics(scan, psm, statistics):
    return [statistic.calculate(scan, psm) for statistic in statistics]

