import sys
import re
import sqlite3
from .psm import PsmManager
from .peak_list import ScanSourceManager

## Tables
CREATE_Source_TABLE = """
CREATE TABLE Source (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 name TEXT,
 format TEXT,
 spectrumIDFormat TEXT,
 location TEXT
)
"""
CREATE_SpectraData_TABLE = """
CREATE TABLE SpectraData (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 name TEXT,
 format TEXT,
 spectrumIDFormat TEXT,
 location TEXT
)
"""
CREATE_SearchDatabase_TABLE = """
CREATE TABLE SearchDatabase (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 name TEXT,
 format TEXT,
 numDatabaseSequences INTEGER,
 numResidues INTEGER,
 releaseDate TEXT,
 version TEXT,
 location TEXT
)
"""
CREATE_DBSequence_TABLE = """
CREATE TABLE DBSequence (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 id INTEGER,
 SearchDatabase_pkid INTEGER REFERENCES SearchDatabase(pkid),
 accession TEXT,
 description TEXT,
 length INTEGER,
 sequence TEXT
)
"""
CREATE_PeptideEvidence_TABLE = """
CREATE TABLE PeptideEvidence (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 SpectrumIdentification_pkid INTEGER REFERENCES SpectrumIdentification(pkid),
 Peptide_pkid INTEGER REFERENCES Peptide(pkid),
 DBSequence_pkid INTEGER REFERENCES DBSequence(pkid),
 isDecoy INTEGER,
 pre TEXT,
 post TEXT,
 start INTEGER,
 end INTEGER
)
"""
CREATE_Peptide_TABLE = """
CREATE TABLE Peptide (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 sequence TEXT,
 modNum INTEGER
)
"""
CREATE_Modification_TABLE = """
CREATE TABLE Modification (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 Peptide_pkid INTEGER REFERENCES Peptide(pkid),
 location INTEGER,
 residues TEXT,
 replacementResidue TEXT,
 name TEXT,
 avgMassDelta REAL,
 monoisotopicMassDelta REAL 
)
"""
CREATE_Score_TABLE = """
CREATE TABLE Score (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 spectrum_identification_id TEXT,
 SpectrumIdentification_pkid INTEGER REFERENCES SpectrumIdentification(pkid) 
)
"""
CREATE_SpectrumIdentification_TABLE = """
CREATE TABLE SpectrumIdentification (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 Spectrum_pkid INTEGER,
 spectrum_id TEXT,
 acquisitionNum INTEGER,
 chargeState INTEGER,
 retentionTime REAL,
 rank INTEGER,
 passThreshold INTEGER,
 experimentalMassToCharge REAL,
 calculatedMassToCharge REAL
)
"""
### String CREATE_Fragmentation_TABLE = """
### CREATE TABLE Fragmentation (
 ### pkid INTEGER PRIMARY KEY,
 ### spectrum_identification_id TEXT,
 ### charge INTEGER,
 ### index TEXT
### )
### """
CREATE_Spectrum_TABLE = """
CREATE TABLE Spectrum (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 id TEXT,
 title TEXT,
 acquisitionNum INTEGER,
 msLevel INTEGER,
 polarity INTEGER,
 peaksCount INTEGER,
 totIonCurrent REAL,
 retentionTime REAL,
 basePeakMZ REAL,
 basePeakIntensity REAL,
 collisionEnergy REAL,
 ionisationEnergy REAL,
 lowMZ REAL,
 highMZ REAL,
 precursorScanNum INTEGER,
 precursorCharge INTEGER,
 precursorMZ REAL,
 precursorIntensity REAL 
)
"""
CREATE_Peaks_TABLE = """
CREATE TABLE Peaks (
 pkid INTEGER PRIMARY KEY ASC AUTOINCREMENT,
 Spectrum_pkid INTEGER REFERENCES Spectrum(pkid),
 acquisitionNum INTEGER,
 moz TEXT,
 intensity TEXT
)
"""
## Indexes
CREATE_DBSequence_DB_Accession_INDEX = """
CREATE INDEX DBSequence_db_accession_idx ON DBSequence (SearchDatabase_pkid,accession)
"""
CREATE_DBSequence_Accession_INDEX = """
CREATE INDEX DBSequence_acc_idx ON DBSequence (accession)
"""
CREATE_Peptide_sequence_INDEX = """
CREATE INDEX Peptide_sequence_idx ON Peptide (sequence)
"""
CREATE_PeptideEvidence_FKEYs_INDEX = """
CREATE INDEX PeptideEvidence_fkey_idx on PeptideEvidence (spectrumidentification_pkid,dbsequence_pkid,peptide_pkid)
"""
CREATE_Spectrum_msLevel_INDEX = """
CREATE INDEX Spectrum_msLevel_idx  on Spectrum (msLevel)
"""
CREATE_Spectrum_polarity_INDEX = """
CREATE INDEX Spectrum_polarity_idx on Spectrum (polarity)
"""
CREATE_Spectrum_peaksCount_INDEX = """
CREATE INDEX Spectrum_peaksCount_idx on Spectrum (peaksCount)
"""
CREATE_Spectrum_totIonCurrent_INDEX = """
CREATE INDEX Spectrum_totIonCurrent_idx on Spectrum (totIonCurrent)
"""
CREATE_Spectrum_retentionTime_INDEX = """
CREATE INDEX Spectrum_retentionTime_idx on Spectrum (retentionTime)
"""
CREATE_Spectrum_basePeakMZ_INDEX = """
CREATE INDEX Spectrum_basePeakMZ_idx on Spectrum (basePeakMZ)
"""
CREATE_Spectrum_basePeakIntensity_INDEX = """
CREATE INDEX Spectrum_basePeakIntensity_idx on Spectrum (basePeakIntensity)
"""
CREATE_Spectrum_collisionEnergy_INDEX = """
CREATE INDEX Spectrum_collisionEnergy_idx on Spectrum (collisionEnergy)
"""
CREATE_Spectrum_ionisationEnergy_INDEX = """
CREATE INDEX Spectrum_ionisationEnergy_idx on Spectrum (ionisationEnergy)
"""
CREATE_Spectrum_lowMZ_INDEX = """
CREATE INDEX Spectrum_lowMZ_idx on Spectrum (lowMZ)
"""
CREATE_Spectrum_highMZ_INDEX = """
CREATE INDEX Spectrum_highMZ_idx on Spectrum (highMZ)
"""
CREATE_Spectrum_precursorCharge_INDEX = """
CREATE INDEX Spectrum_precursorCharge_idx on Spectrum (precursorCharge)
"""
CREATE_Spectrum_precursorIntensity_INDEX = """
CREATE INDEX Spectrum_precursorIntensity_idx on Spectrum (precursorIntensity)
"""

CREATE_SpectrumIdentification_chargeState_INDEX = """
CREATE INDEX SpectrumIdentification_chargeState_idx  on SpectrumIdentification (chargeState)
"""
CREATE_SpectrumIdentification_retentionTime_INDEX = """
CREATE INDEX SpectrumIdentification_retentionTime_idx  on SpectrumIdentification (retentionTime)
"""
CREATE_SpectrumIdentification_experimentalMassToCharge_INDEX = """
CREATE INDEX SpectrumIdentification_experimentalMassToCharge_idx  on SpectrumIdentification (experimentalMassToCharge)
"""
CREATE_SpectrumIdentification_calculatedMassToCharge_INDEX = """
CREATE INDEX SpectrumIdentification_calculatedMassToCharge_idx  on SpectrumIdentification (calculatedMassToCharge)
"""
    
"""
    public static final String[] TABLE_DEFS = {CREATE_Source_TABLE, CREATE_SpectraData_TABLE, CREATE_SearchDatabase_TABLE, CREATE_DBSequence_TABLE, CREATE_Peptide_TABLE, CREATE_PeptideEvidence_TABLE, CREATE_Modification_TABLE, CREATE_SpectrumIdentification_TABLE, CREATE_Score_TABLE, CREATE_Spectrum_TABLE,CREATE_Peaks_TABLE};
    public static final String[] INDEX_DEFS = {CREATE_DBSequence_Accession_INDEX, CREATE_Peptide_sequence_INDEX, CREATE_PeptideEvidence_FKEYs_INDEX};
"""

TABLE_DEFS = [ CREATE_Source_TABLE, CREATE_SpectraData_TABLE, CREATE_SearchDatabase_TABLE, CREATE_DBSequence_TABLE, CREATE_Peptide_TABLE, CREATE_PeptideEvidence_TABLE, CREATE_Modification_TABLE, CREATE_SpectrumIdentification_TABLE, CREATE_Score_TABLE, CREATE_Spectrum_TABLE, CREATE_Peaks_TABLE ]
INDEX_DEFS = [ CREATE_DBSequence_Accession_INDEX, CREATE_Peptide_sequence_INDEX, CREATE_PeptideEvidence_FKEYs_INDEX]

TABLE_FIELDS = {\
'Source':('pkid','name','format','spectrumIDFormat','location'),\
'SpectraData':('pkid','name','format','spectrumIDFormat','location'),\
'Spectrum':('pkid','id','title','acquisitionNum','msLevel','polarity','peaksCount','totIonCurrent','retentionTime','basePeakMZ','basePeakIntensity','collisionEnergy','ionisationEnergy','lowMZ','highMZ','precursorScanNum','precursorCharge','precursorMZ','precursorIntensity'),\
'Peaks':('pkid','Spectrum_pkid','acquisitionNum','moz','intensity'),\
'SearchDatabase':('pkid','name','format','numDatabaseSequences','numResidues','releaseDate','version','location'),\
'DBSequence':('pkid','id','SearchDatabase_pkid','accession','description','length','sequence'),\
'Peptide':('pkid','sequence','modNum'),\
'Modification':('pkid','Peptide_pkid','location','residues','replacementResidue','name','avgMassDelta','monoisotopicMassDelta'),\
'PeptideEvidence':('pkid','SpectrumIdentification_pkid','Peptide_pkid','DBSequence_pkid','isDecoy','pre','post','start','end'),\
'SpectrumIdentification':('pkid','Spectrum_pkid','spectrum_id','acquisitionNum','chargeState','retentionTime','rank','passThreshold','experimentalMassToCharge','calculatedMassToCharge'),\
'Score':('pkid','spectrum_identification_id','SpectrumIdentification_pkid')\
}

TABLE_FIELD_TYPES = {\
'DBSequence' : {'pkid' : 'INTEGER','id' : 'INTEGER','SearchDatabase_pkid' : 'INTEGER','accession' : 'TEXT','description' : 'TEXT','length' : 'INTEGER','sequence' : 'TEXT'},\
'Modification' : {'pkid' : 'INTEGER','Peptide_pkid' : 'INTEGER','location' : 'INTEGER','residues' : 'TEXT','replacementResidue' : 'TEXT','name' : 'TEXT','avgMassDelta' : 'REAL','monoisotopicMassDelta' : 'REAL'},\
'Peaks' : {'pkid' : 'INTEGER','spectrum_pkid' : 'INT','acquisitionNum' : 'INTEGER','moz' : 'TEXT','intensity' : 'TEXT'},\
'Peptide' : {'pkid' : 'INTEGER','sequence' : 'TEXT','modNum' : 'INTEGER'},\
'PeptideEvidence' : {'pkid' : 'INTEGER','SpectrumIdentification_pkid' : 'INTEGER','Peptide_pkid' : 'INTEGER','DBSequence_pkid' : 'INTEGER','isDecoy' : 'INTEGER','pre' : 'TEXT','post' : 'TEXT','start' : 'INTEGER','end' : 'INTEGER'},\
'Score' : {'pkid' : 'INTEGER','spectrum_identification_id' : 'TEXT','SpectrumIdentification_pkid' : 'INTEGER'},\
'SearchDatabase' : {'pkid' : 'INTEGER','name' : 'TEXT','format' : 'TEXT','numDatabaseSequences' : 'INTEGER','numResidues' : 'INTEGER','releaseDate' : 'TEXT','version' : 'TEXT','location' : 'TEXT'},\
'Source' : {'pkid' : 'INTEGER','name' : 'TEXT','format' : 'TEXT','spectrumIDFormat' : 'TEXT','location' : 'TEXT'},\
'SpectraData' : {'pkid' : 'INTEGER','name' : 'TEXT','format' : 'TEXT','spectrumIDFormat' : 'TEXT','location' : 'TEXT'},\
'Spectrum' : {'pkid' : 'INTEGER','id' : 'TEXT','title' : 'TEXT','acquisitionNum' : 'INTEGER','msLevel' : 'INTEGER','polarity' : 'INTEGER','peaksCount' : 'INTEGER','totIonCurrent' : 'REAL','retentionTime' : 'REAL','basePeakMZ' : 'REAL','basePeakIntensity' : 'REAL','collisionEnergy' : 'REAL','ionisationEnergy' : 'REAL','lowMZ' : 'REAL','highMZ' : 'REAL','precursorScanNum' : 'INTEGER','precursorCharge' : 'INTEGER','precursorMZ' : 'REAL','precursorIntensity' : 'REAL'},\
'SpectrumIdentification' : {'pkid' : 'INTEGER','Spectrum_pkid' : 'INTEGER','spectrum_id' : 'TEXT','acquisitionNum' : 'INTEGER','chargeState' : 'INTEGER','retentionTime' : 'REAL','rank' : 'INTEGER','passThreshold' : 'INTEGER','experimentalMassToCharge' : 'REAL','calculatedMassToCharge' : 'REAL'}\
}

Score_FIELD_TYPES = ['INTEGER','INTEGER','TEXT']
SCOREFILTER = ['PeptideEvidenceRef','PeptideSequence','Modification','IonType','peptide', 'scan_id', 'scan_index', 'scan_number','source_statistic'] + list(TABLE_FIELDS['SpectrumIdentification']) + list(TABLE_FIELDS['PeptideEvidence']) + list(TABLE_FIELDS['Score'])


class SQLiteDB(object):
    conn = None
    sqlfile = None

    def __init__(self,sqlfile):
        if sqlfile:
            self.create_db(sqlfile)

    def execute(self,stmt,vals,debug=False):
      if debug: print >> sys.stderr, "stmt: %s %s\n" % (stmt,vals if vals else '')
      try:
          c = conn.cursor()
          if vals:
              c.execute(stmt,vals)
          else:
              c.execute(stmt)
          lastrowid = c.lastrowid
          conn.commit()
          c.close()
          return lastrowid
      except Exception as e:
          print >> sys.stderr, "execute: %s\n  %s\n  %s\n\n" % (e,stmt,vals)
      return None

    def get_insert_stmt(self,table):
        if table in TABLE_FIELDS:
            fields = TABLE_FIELDS[table]
            return 'INSERT INTO %s%s VALUES(%s)' % (table,fields,','.join([ "?" for x in fields]))
        return None

    def insert_row(self,table,vals):
        stmt = self.get_insert_stmt(table)
        if stmt:
            return self.execute(stmt,vals)
        return None

    def create_db(self,sqlfile):
        self.sqlfile = sqlfile
        try:
            if not self.conn:
                self.conn = sqlite3.connect(self.sqlfile)
            for tbldef in TABLE_DEFS:
                self.execute(tbldef,None)
            for idxdef in INDEX_DEFS:
                self.execute(idxdef,None)
            return conn
        except Exception as e:
            print >> sys.stderr, "create_db: %s" % e

conn = None

def execute(stmt,vals,debug=False):
  if debug: print >> sys.stdout, "stmt: %s %s\n" % (stmt,vals if vals else '')
  try:
      c = conn.cursor()
      if vals:
          c.execute(stmt,vals)
      else:
          c.execute(stmt)
      lastrowid = c.lastrowid
      conn.commit()
      c.close()
      return lastrowid
  except Exception as e:
      print >> sys.stderr, "execute: %s\n  %s\n  %s\n\n" % (e,stmt,vals)
  return None

def add_table_column(table,column_defs):
    ## print >> sys.stdout, "add_table_column: %s\n" % str(column_defs)
    for name,colType in column_defs:
        ## print >> sys.stdout, "add_table_column: %s\t%s\t%s\n" % (name,colType,TABLE_FIELDS[table])
        if name not in TABLE_FIELDS[table]:
            stmt = "ALTER TABLE Score ADD COLUMN [%s]  %s" % (name,colType)
            execute(stmt,None,debug=False)
            TABLE_FIELDS[table] = tuple(list(TABLE_FIELDS[table]) + [name])
            TABLE_FIELD_TYPES[table][name] = colType

def get_insert_stmt(table):
    if table in TABLE_FIELDS:
        fields = TABLE_FIELDS[table]
        return 'INSERT INTO %s%s VALUES(%s)' % (table,fields,','.join([ "?" for x in fields]))
    return None

def insert_row(table,vals):
    stmt = get_insert_stmt(table)
    if stmt:
        return execute(stmt,vals,debug=False)
    return None

def create_table_column_index(table,columns):
    for col_def in columns: 
        coldefs = col_def if isinstance(col_def,list) else [col_def]
        col_names = [str(x).lstrip('[').rstrip(']') for x in coldefs]
        cols = ','.join([name if re.match('^\w+$',name) else '[%s]' % name for name in col_names])
        idx_name = "%s_%s_idx" % (table,'_'.join(col_names))
        stmt = "CREATE INDEX %s on %s (%s)" % (idx_name if re.match('^\w+$',idx_name) else '[%s]' % idx_name, table,cols)
        try:
            execute(stmt,None);
        except Exception as e:
            print >> sys.stderr, "create_table_column_index: %s" % e

def create_db(sqlfile):
    global conn
    try:
        if not conn:
            conn = sqlite3.connect(sqlfile)
        for tbldef in TABLE_DEFS:
            execute(tbldef,None,debug=True)
        for idxdef in INDEX_DEFS:
            execute(idxdef,None)
        return conn
    except Exception as e:
        print >> sys.stderr, "create_db: %s" % e

def getDSequenceID():
        pass

def sqldb(settings,scan_source_manager,psm_manager,collected_names,collected_statistics,source_statistic_names):
    sqlfile = settings.get('sqlitedb', None)
    if not sqlfile:
        return
    print >> sys.stderr, "source_statistic_names: %s" % source_statistic_names
    print >> sys.stderr, "collected_names: %s" % collected_names
    print >> sys.stderr, "collected_statistics: %s" % collected_statistics
    create_db(sqlfile)
    scancnt = 0
    scandbcnt = 0
    psmcnt = 0
    for scan in scan_source_manager.get_scans():
        scancnt += 1
        #Spectrum_pkid = insert_scan(scan)
        Spectrum_pkid = None
        for psm in psm_manager.psms_for_scan(scan):
            if Spectrum_pkid is None:
                Spectrum_pkid = insert_scan(scan)
                scandbcnt +=1
            insert_psm(psm,Spectrum_pkid,collected_names,collected_statistics[psmcnt],source_statistic_names)
            psmcnt += 1
    print >> sys.stdout, "PSMs: %d Scans %d of %d" % (psmcnt,scandbcnt,scancnt)
    create_table_column_index('Spectrum',['acquisitionNum', 'msLevel', 'polarity', 'peaksCount', 'totIonCurrent', 'retentionTime', 'basePeakMZ', 'basePeakIntensity', 'collisionEnergy', 'ionisationEnergy', 'lowMZ', 'highMZ', 'precursorScanNum', 'precursorCharge', 'precursorMZ', 'precursorIntensity'])
    scorefields = set(TABLE_FIELDS['Score']) - set(SCOREFILTER)
    create_table_column_index('Score',scorefields)

# SourceRefs scan.source.name -> pkid 
SourceRefs = dict()
# ScanDicts [source.pkid] [scan.index] -> pkid
ScanDicts = dict()
def insert_sourcedb(scan):
    #name = scan.source.get('name',None)
    name = scan.source.name
    pkid = SourceRefs.get(name,None)
    if not pkid:
        fileFormat = None
        spectrumIDFormat = None
        #location = scan.source.get('path',None)
        location = scan.source.path
        #Source(pkid,name,format,spectrumIDFormat,location)
        values = (None,name,fileFormat,spectrumIDFormat,location)
        pkid = insert_row('Source',values)
        SourceRefs[name] = pkid
        ScanDicts[pkid] = dict()
    return pkid
scan_cnt = 0
dbg_scan_cnt = 0
def insert_scan(scan):
    global scan_cnt
    Spectrum_pkid = ScanDicts[insert_sourcedb(scan)].get(scan.index,None)
    if Spectrum_pkid is None:
        scan_cnt += 1
        if scan_cnt < dbg_scan_cnt:
            print >> sys.stdout, "scan:\t%s,%s,%s,%s,%s"% ( scan.id, scan.source.name, scan.index, scan.number, scan.base_peak_mz )
        ## Source
        ## Spectrum (pkid INTEGER PRIMARY KEY, id TEXT, acquisitionNum INTEGER, msLevel INTEGER, polarity INTEGER, peaksCount INTEGER, totIonCurrent REAL, retentionTime REAL, basePeakMZ REAL, basePeakIntensity REAL, collisionEnergy REAL, ionisationEnergy REAL, lowMZ REAL, highMZ REAL, precursorScanNum INTEGER, precursorCharge INTEGER, precursorMZ REAL, precursorIntensity REAL )
        #Spectrum(pkid,     id, title, acquisitionNum, msLevel, polarity, peaksCount,         totIonCurrent, retentionTime, basePeakMZ,        basePeakIntensity, collisionEnergy, ionisationEnergy, lowMZ, highMZ, precursorScanNum, precursorCharge, precursorMZ, precursorIntensity )
        mz_array = [x for x in scan.mz_array]
        intensity_array = [x for x in scan.intensity_array]
        values = (None,scan.id, scan.id, scan.number, scan.ms_level, None, len(mz_array), scan.total_ion_current, scan.retention_time, scan.base_peak_mz,  scan.base_peak_intensity,              None, None, None,  None,   scan.precursor_scan_num, scan.precursor_charge, scan.precursor_mz, scan.precursor_intensity )
        if scan_cnt < dbg_scan_cnt:
            print >> sys.stdout, "spec:\t%s"%  str(values)
        Spectrum_pkid = insert_row('Spectrum',values)
        ## Peaks
        #Peaks   (pkid,     id,spectrum_pkid,acquisitionNum,          moz,           intensity)
        values = (None,Spectrum_pkid,   scan.number,str(mz_array),str(intensity_array))
        if scan_cnt < dbg_scan_cnt:
            print >> sys.stdout, "peak:\t%s"% ( str(values) )
            sys.stdout.flush()
        pkid = insert_row('Peaks',values)
    return Spectrum_pkid

psm_cnt = 0
dbg_psm_cnt = 0
def insert_psm(psm,Spectrum_pkid,collected_names,collected_statistics,source_statistic_names):
    global psm_cnt
    labeled_sequence = psm.peptide.labeled_sequence_parts
    if psm_cnt < dbg_psm_cnt:
        print >> sys.stdout, "psm:\t%s,%s,( %s ) \tscores:\n %s\n %s\n %s\n" % ( psm.scan_reference.id, psm.peptide.sequence, str(labeled_sequence),psm.source_statistics,source_statistic_names,collected_statistics)
    psm_cnt += 1
    #print >> sys.stdout, "psm:\t%s,%s,( %s ) \tscores: %s" % ( psm.scan_reference.id, psm.peptide.sequence, str(labeled_sequence), str(psm.peptide.modifications), psm.source_statistics)
    """
    identification_item: {
      'PeptideEvidenceRef': [
        {'pre': 'R', 'protein description': 'ECA3359 30S ribosomal protein S16', 'isDecoy': False, 'end': 51, 'numDatabaseSequences': '4499', 'accession': 'ECA3359', 'start': 36, 'length': 82, 'PeptideSequence': 'VGFFNPIATGQAEALR', 'location': '/home/lgatto/bin/MSGFPlus-20130828/erwinia_carotovora.fasta', 'DatabaseName': 'erwinia_carotovora.fasta', 'post': 'L', 'FileFormat': 'FASTA format'}
       ], 
      'passThreshold': True, 
      'IsotopeError': 0.0, 
      'rank': 1, 'chargeState': 2, 
      'calculatedMassToCharge': 845.94921875, 
      'experimentalMassToCharge': 845.9484252929688, 
      'PeptideSequence': 'VGFFNPIATGQAEALR', 
      'AssumedDissociationMethod': 'HCD', 
      'MS-GF:DeNovoScore': 163.0, 
      'MS-GF:RawScore': 158.0, 
      'MS-GF:EValue': 1.3061375e-12, 
      'MS-GF:SpecEValue': 9.10043e-19
    }
    """
    """
    SearchDatabase(pkid,id,name,format,numDatabaseSequences,numResidues,releaseDate,version,location)
    DBSequence(pkid,id,SearchDatabase_pkid,accession,description,length,sequence)
    Peptide(pkid,id,sequence,modNum)
    Modification(pkid,id,Peptide_pkid,location,residues,replacementResidue,name,avgMassDelta,monoisotopicMassDelta)
    PeptideEvidence(pkid,SpectrumIdentification_pkid,Peptide_pkid,DBSequence_pkid,isDecoy,pre,post,start,end)
    Score:(pkid,spectrum_identification_id,SpectrumIdentification_pkid]\
    """
    ## SpectrumIdentification
    rank = psm.source_statistics.get('rank',None)
    calculatedMassToCharge = psm.source_statistics.get('calculatedMassToCharge',None)
    experimentalMassToCharge = psm.source_statistics.get('experimentalMassToCharge',None)
    passThreshold = psm.source_statistics.get('passThreshold',None)
    #SpectrumIdentification(pkid,  Spectrum_pkid,          spectrum_id,           acquisitionNum,chargeState,retentionTime,rank,passThreshold,experimentalMassToCharge,calculatedMassToCharge)
    values =               (None,Spectrum_pkid,psm.scan_reference.id,psm.scan_reference.number,       None,         None,rank,passThreshold,experimentalMassToCharge,calculatedMassToCharge)
    SpectrumIdentification_pkid = insert_row('SpectrumIdentification',values)
    ## Peptide
    Peptide_pkid = insert_peptide(psm)
    ## PeptideEvidence
    peptideEvidenceRefs = psm.source_statistics.get('PeptideEvidenceRef',None)
    if peptideEvidenceRefs:
        for peptideEvidence in peptideEvidenceRefs:
            DBSequence_pkid = insert_dbsequence(peptideEvidence)
            isDecoy = peptideEvidence.get('isDecoy',None)
            pre = peptideEvidence.get('pre',None)
            post = peptideEvidence.get('post',None)
            start = peptideEvidence.get('start',None)
            end = peptideEvidence.get('end',None)
            #PeptideEvidence(pkid,SpectrumIdentification_pkid,Peptide_pkid,DBSequence_pkid,isDecoy,pre,post,start,end)
            values = (None,SpectrumIdentification_pkid,Peptide_pkid,DBSequence_pkid,isDecoy,pre,post,start,end)
            pkid = insert_row('PeptideEvidence',values)
    ## scores
    scorefields = (set(collected_names) | set(psm.source_statistics.keys())) - set(SCOREFILTER)
    newfields = scorefields - set(TABLE_FIELDS['Score'])
    if psm_cnt < dbg_psm_cnt:
        print >> sys.stdout, "scorefields: %s  newfields: %s" % (str(scorefields),str(newfields))
    if len(newfields) > 0: 
       ## print >> sys.stdout, "source_statistics: %s\n" % (psm.source_statistics)
       ## print >> sys.stdout, "collected_names: %s\n" % (collected_names)
       ## print >> sys.stdout, "collected_statistics: %s\n" % (collected_statistics)
       columndefs = []
       for field in sorted(newfields):
           if field in psm.source_statistics:
               val = psm.source_statistics.get(field, None)
           elif field in collected_names:
               val = collected_statistics[collected_names.index(field)]
           else:
               val = None
           value = collected_statistics[collected_names.index(field)] if field in collected_names else psm.source_statistics.get(field,None)
           colType = 'REAL' if isinstance(val,int) or isinstance(val,float) or field == 'total_ion_current' else 'TEXT'
           ## print >> sys.stdout, "newcol: %s %s %s %s\n" % (field, colType, val, value)
           if val or 0. == val:
               columndefs.append([field,colType])
       add_table_column('Score',columndefs)
    score_values = []
    for field in TABLE_FIELDS['Score']:
        if field == 'pkid':
            val = None
        elif field == 'spectrum_identification_id':
            val = psm.scan_reference.id
        elif field == 'SpectrumIdentification_pkid':
            val = SpectrumIdentification_pkid
        else:
            if field in psm.source_statistics:
                val = psm.source_statistics.get(field, None)
            elif field in collected_names:
                val = collected_statistics[collected_names.index(field)]
            else:
                val = None
        if val or 0. == val:
            colType = TABLE_FIELD_TYPES['Score'][field]
            val = float(val) if  colType == 'REAL' else int(val) if colType == 'INTEGER' else str(val)
        score_values.append(val)
    pkid = insert_row('Score',score_values)

# peptideRefs psm.peptide.labeled_sequence_parts -> pkid
peptideRefs = dict()
def insert_peptide(psm):
    labeled_sequence = str(psm.peptide.labeled_sequence_parts)
    pkid = peptideRefs.get(labeled_sequence,None)
    if not pkid:
        modstring = psm.source_statistics.get('Modification',None)
        mods = modstring if modstring else []
        modNum = len(mods)
        sequence = psm.peptide.sequence
        modifications =  psm.peptide.modifications
        n_term_modifications = psm.peptide.n_term_modifications
        c_term_modifications = psm.peptide.c_term_modifications
        # Peptide(pkid,sequence,modNum)
        values = (None,sequence,modNum)
        pkid = insert_row('Peptide',values)
        peptideRefs[labeled_sequence] = pkid
        ## Modifications
        for mod in mods:
            # {'monoisotopicMassDelta': 57.021463735, 'location': 4, 'name': 'Carbamidomethyl'}
            location = mod.get('location',None)
            monoisotopicMassDelta = mod.get('monoisotopicMassDelta',None)
            avgMassDelta = mod.get('avgMassDelta',None)
            name =  mod.get('name',None)
            if not name:
              keys = set(mod.keys()) - set(['name','location','monoisotopicMassDelta','avgMassDelta','replacementResidue','originalResidue','residues'])
              name = list(keys)[0] if keys else None 
            replacementResidue = mod.get('replacementResidue',None)
            residues = None
            residuesval = mod.get('originalResidue',None) if replacementResidue else mod.get('residues',None)
            if residuesval:
                if isinstance(residuesval,list):
                    residues = residuesval[0] if len(residuesval) == 1 else str(residuesval)
                else:
                    residues = residuesval
            #Modification(pkid,Peptide_pkid,location,residues,replacementResidue,name,avgMassDelta,monoisotopicMassDelta)
            values = (None,pkid,location,residues,replacementResidue,name,avgMassDelta,monoisotopicMassDelta)
            Mod_pkid = insert_row('Modification',values)
            peptideRefs[labeled_sequence] = pkid
    return pkid

# SearchDbRefs location -> pkid 
SearchDbRefs = dict()
# pkid -> {acessession:pkid}
SearchDbDicts = dict()
def insert_searchdb(peptideEvidence):
    location =  peptideEvidence.get('location')
    pkid = SearchDbRefs.get(location,None)
    if not pkid:
        name = peptideEvidence.get('DatabaseName',None)
        fileFormat = peptideEvidence.get('FileFormat',None)
        numDatabaseSequences = peptideEvidence.get('numDatabaseSequences',None)
        numResidues = peptideEvidence.get('numResidues',None)
        releaseDate = peptideEvidence.get('releaseDate',None)
        version = peptideEvidence.get('version',None)
        #SearchDatabase(pkid,name,format,numDatabaseSequences,numResidues,releaseDate,version,location)
        values = (None,name,fileFormat,numDatabaseSequences,numResidues,releaseDate,version,location)
        pkid = insert_row('SearchDatabase',values)
        SearchDbRefs[location] = pkid
        SearchDbDicts[pkid] = dict()
    return pkid

def insert_dbsequence(peptideEvidence):
    SearchDatabase_pkid = insert_searchdb(peptideEvidence)
    accession = peptideEvidence.get('accession')
    pkid = SearchDbDicts[SearchDatabase_pkid].get(accession,None)
    if not pkid:
        description = peptideEvidence.get('protein description',None)
        name = peptideEvidence.get('name',None)
        length = peptideEvidence.get('length',None)
        sequence = peptideEvidence.get('Seq',None)
        #DBSequence(pkid,id,SearchDatabase_pkid,accession,description,length,sequence)
        values = (None,None,SearchDatabase_pkid,accession,description,length,sequence)
        pkid = insert_row('DBSequence',values)
        SearchDbDicts[SearchDatabase_pkid][accession] = pkid
    return pkid

