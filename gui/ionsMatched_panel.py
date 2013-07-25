import wx
from numPeaks_panel import *

class ionsMatchedPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent, name='ionMatched')
        self.SetAutoLayout(True)
        
        # set parent
        self.parent = parent
        
        # set sizer
        self.grid = wx.BoxSizer(wx.VERTICAL)
        self.SetSizer(self.grid)
        self.grid.AddSpacer(5,5)
        
        self.ionlossdict = {}
        
        self.aggregateType = ['count', 'count_missed', 'count_longest_stretch', 'percent', 'percent_missed', 'list_matches', 'list_misses']
        
        # Peak Matching Stats
        self.stats = ['Number of Matched Ions', 'Number of Unmatched Ions', 'Longest Stretch of Matched Ions', 'Percent of Matched Ions', 'Percent of Unmatched Ions', 'List Matched Ions', 'List Unmatched Ions']
        self.statsLbl = wx.StaticText(self, label="Ion Matching Statistic:")
        self.grid.Add(self.statsLbl)
        self.editStatsLbl = wx.ComboBox(self, size=(-1, -1), choices=self.stats, style=wx.CB_DROPDOWN)
        self.Bind(wx.EVT_COMBOBOX, self.EvtStats, self.editStatsLbl)
        self.grid.Add(self.editStatsLbl)

        # set default values
        self.editStatsLbl.SetValue(self.stats[0])
        self.parent.colVal['aggregate_by'] = self.aggregateType[0]

        self.grid.AddSpacer(5,5)
        
        # Ion series
        self.ions = ['a1', 'a2', 'a3', 'b1', 'b2', 'b3', 'c1', 'c2', 'c3', 'x1', 'x2', 'x3', 'y1', 'y2', 'y3', 'z1', 'z2', 'z3', 'M1', 'M2', 'Internal Ions']
        self.ionLbl = wx.StaticText(self, label="Ion Series:")
        self.grid.Add(self.ionLbl)
        self.editIonsLbl = wx.CheckListBox(self, size=(-1, -1), choices=self.ions)
        self.Bind(wx.EVT_CHECKLISTBOX, self.EvtIons, self.editIonsLbl)
        self.grid.Add(self.editIonsLbl)
        
        # set default value to none selected
        self.seriesVal = []
        self.ionlossdict['series'] = self.seriesVal

        self.grid.AddSpacer(5,5)

        # Losses
        self.losses = ['H2O', 'NH3', 'CO (-28) on internal ions']
        self.lossLbl = wx.StaticText(self, label="Losses:")
        self.grid.Add(self.lossLbl)
        self.editLossLbl = wx.ListBox(self, size=(-1,-1), choices=self.losses, style=wx.LB_EXTENDED)
        self.Bind(wx.EVT_LISTBOX, self.EvtLosses, self.editLossLbl)
        self.grid.Add(self.editLossLbl)
        
        # set default to none selected
        self.lossesVal = []
        self.ionlossdict['losses'] = self.lossesVal

        self.grid.AddSpacer(5,5)

        # Peak Filters
        self.grid.Add(numPeaksPanel(self), wx.EXPAND)
        self.Fit()
        self.parent.Fit()
        self.grid.AddSpacer(5,5)

        # Mass Tolerance
        self.massTolerance = wx.CheckBox(self, size=(-1,-1), label='Specify Mass Tolerance:')
        self.Bind(wx.EVT_CHECKBOX, self.EvtMassTolerance, self.massTolerance)
        self.grid.Add(self.massTolerance)

        self.grid.AddSpacer(5,5)
        self.Show(True)

        # Mass Tolerance TextCtrl
        self.lblMass = wx.StaticText(self, label="Tolerance:")
        self.grid.Add(self.lblMass)
        self.editMass = wx.TextCtrl(self, value="0.5")
        self.Bind(wx.EVT_TEXT, self.EvtMass, self.editMass)
        
        # hide the widget until checked
        self.grid.Add(self.editMass)
        self.grid.Hide(self.lblMass)
        self.grid.Hide(self.editMass)
        self.grid.AddSpacer(5,5)
        
        # update col title
        self.updateName()

# ================================================================================ #

# Events

    # helper function for handling error while changing strings entered to numeric values
    def toFloat(self, value):
        try:
            return float(value)
        except exceptions.ValueError:
            print "field empty or contains invalid character: using 0.0"
            return 0.0

    # ----

    def EvtMass(self, event):
        self.parent.colVal['mass_tolerance'] = self.toFloat(self.editMass.GetValue())

    # ----

    def EvtMassTolerance(self, event):
        if self.massTolerance.GetValue():
            if self.lblMass.IsShown() == False:
                self.grid.Show(self.lblMass)
                self.Fit()
                self.parent.Fit()
                self.parent.parent.FitInside()
                self.parent.parent.parent.Layout()
            if self.editMass.IsShown() == False:
                self.grid.Show(self.editMass)
                self.Fit()
                self.parent.Fit()
                self.parent.parent.FitInside()
                self.parent.parent.parent.Layout()
            self.parent.colVal['mass_tolerance'] = float(self.editMass.GetValue())
        else:
            if self.lblMass.IsShown():
                self.grid.Hide(self.lblMass)
                self.Fit()
                self.parent.Fit()
                self.parent.parent.FitInside()
                self.parent.parent.parent.Layout()
            if self.editMass.IsShown():
                self.grid.Hide(self.editMass)
                self.Fit()
                self.parent.Fit()
                self.parent.parent.FitInside()
                self.parent.parent.parent.Layout()
            del self.parent.colVal['mass_tolerance']

    # ----

    def EvtLosses(self, event):
        if 'ions_ref' in self.parent.colVal: del self.parent.colVal['ions_ref']
        self.lossesVal = [self.losses[i] for i in self.editLossLbl.GetSelections()]
        self.ionlossdict['losses'] = self.lossesVal
        self.parent.colVal['ions'] = self.ionlossdict

    # ----

    def EvtIons(self, event):
        if 'ions_ref' in self.parent.colVal: del self.parent.colVal['ions_ref']
        self.seriesVal =  self.seriesVal = [self.ions[i] for i in self.editIonsLbl.GetChecked()]
        self.ionlossdict['series'] = self.seriesVal
        self.parent.colVal['ions'] = self.ionlossdict
        self.updateName()

    # ----
        
    def EvtStats(self, event):
        selection = self.editStatsLbl.GetSelection()
        self.parent.colVal['aggregate_by'] = self.aggregateType[selection]
        self.updateName()

    # ----

    def updateName(self):
        aggType = self.parent.colVal['aggregate_by']
        ionsList = self.ionlossdict['series']
        # TODO: add losses part to the column title
        if aggType == self.aggregateType[0]:
            self.parent.colVal['title'] = 'Matched ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[1]:
            self.parent.colVal['title'] = 'Unmatched ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[2]:
            self.parent.colVal['title'] = 'Longest Stretch of ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[3]:
            self.parent.colVal['title'] = 'Percent of Matched ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[4]:
            self.parent.colVal['title'] = 'Percent of Unmatched ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[5]:
            self.parent.colVal['title'] = 'List of Matched ' + str(ionsList) + ' Ions'
        if aggType == self.aggregateType[6]:
            self.parent.colVal['title'] = 'List of Unmatched ' + str(ionsList) + ' Ions'
        