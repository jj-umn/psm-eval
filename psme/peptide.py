from pyteomics.mass import calculate_mass, std_ion_comp, Composition

ION_COMP = std_ion_comp
ION_COMP['M-CO'] = Composition(formula='C-1O-1')
ION_COMP['M-NH3'] = Composition(formula='N-1H-3')
ION_COMP['M-H2O'] = Composition(formula='H-2O-1')
# pyteomics subtracts another H relative to lorikeet and ProteinProspector,
# Lorikeet & ProteinProspector compute Z-dot ions (though I don't actually
# know what that means).
ION_COMP['z'] = Composition(formula='H-2O-1' + 'ON-1')

H2O = Composition(formula='H2O')


class Peptide(object):

    def __init__(self, sequence, modifications=[], n_term_modifications=[], c_term_modifications=[]):
        self.sequence = sequence
        mods = [[] for _ in range(len(sequence))]
        for modification in modifications:
            index = modification['position']
            if 'unimod_entry' in modification:
                modification = modification['unimod_entry']
            mods[index].append(modification)
        self.modifications = mods
        self.n_term_modifications = n_term_modifications
        self.c_term_modifications = c_term_modifications

    def get_seq_mass(self, start=None, end=None, term="n", **kwds):
        # kwds could be average, ion_type, etc...
        slice = _Slice(self.sequence, start, end, term)
        sub_sequence = self.sub_sequence(slice=slice)
        composition = Composition(parsed_sequence=sub_sequence) + H2O
        # Why is adding H2O needed, is there a more pyteomics way of doing this?
        kwds['ion_comp'] = ION_COMP
        mass = calculate_mass(composition=composition, **kwds)
        mass = self.__add_res_mod_masses(mass, slice, term, **kwds)
        mass = self.__add_term_mod_mass(mass, slice, term, **kwds)
        return mass

    @property
    def labeled_sequence_parts(self):
        aa_and_mods = [(aa, self.__sum_modifications(mod)) for (aa, mod) in zip(self.sequence, self.modifications)]
        return ["%s%s" % (aa, "[%s]" % str(mod) if mod != 0.0 else "") for (aa, mod) in aa_and_mods]

    def sub_sequence(self, start=None, end=None, term=None, slice=None, labeled=False):
        if not slice:
            slice = _Slice(self.sequence, start=start, end=end, term=term)
        sequence = self.sequence if not labeled else self.labeled_sequence_parts
        sub_sequence = "".join(sequence[slice.first_index:slice.last_index])
        return sub_sequence

    def __add_res_mod_masses(self, mass, slice, term, **kwds):
        mass += sum([self.__sum_modifications(pos_modification, **kwds) for pos_modification in self.modifications[slice.first_index:slice.last_index]])
        return mass

    def __add_term_mod_mass(self, mass, slice, term, **kwds):
        if slice.includes_n_term:
            mass += self.__sum_modifications(self.n_term_modifications, **kwds)
        if slice.includes_c_term:
            mass += self.__sum_modifications(self.c_term_modifications, **kwds)
        return mass

    def sum_of_modifications(self, position, **kwds):
        if position == "n":
            sum = self.__sum_modifications(self.n_term_modifications, **kwds)
        elif position == "c":
            sum = self.__sum_modifications(self.c_term_modifications, **kwds)
        else:
            sum = self.__sum_modifications(self.modifications[position], **kwds)
        return sum

    def __sum_modifications(self, modifications, **kwds):
        average = kwds.get('average', False)
        mass_adjust = 0.0
        for modification in modifications:
            if isinstance(modification, (float, int)):
                mod_mass = modification
            elif 'mod_mass' in modification:
                mod_mass = modification['mod_mass']
            else:
                ## Unimod entry
                if average:
                    mod_mass = modification["avge_mass"]
                else:
                    mod_mass = modification["mono_mass"]
            mass_adjust += mod_mass
        return mass_adjust

    def get_seq_mass_avg(self, end, term):
        return self.get_seq_mass(None, end, term, average=True)

    def get_seq_mass_mono(self, end, term):
        return self.get_seq_mass(None, end, term, average=False)


class _Slice(object):

    def __init__(self, sequence, start, end, term):
        if term != "c":
            self.first_index = (0 if start == None else start)
            self.last_index = end
        else:
            self.first_index = end
            self.last_index = len(sequence) if start == None else start
        self.includes_n_term = self.first_index == 0
        self.includes_c_term = self.last_index == len(sequence)

    def __iter__(self):
        return _SliceIter(next)


class _SliceIter(object):

    def __init__(self, slice):
        self.slice = slice
        self.next = slice.first_index

    def next(self):
        if self.next == self.slice.last_index:
            raise StopIteration()
        next = self.next
        self.next += 1
        return next


class PeptideContext(object):

    def __init__(self, peptide, start=None, stop=None, term="n"):
        self.peptide = peptide
        self.start = start
        self.stop = stop
        self.term = term

    def calc_mass(self, **kwds):
        if not self.stop:
            raise Exception
        return self.peptide.get_seq_mass(start=self.start, end=self.stop, term=self.term, **kwds)

    @property
    def sequence(self):
        if not hasattr(self, "_sequence"):
            setattr(self, "_sequence", self.peptide.sub_sequence(self.start, self.stop, self.term))
        return self._sequence

    @property
    def labeled_sequence(self):
        if not hasattr(self, "_lsequence"):
            setattr(self, "_lsequence", self.peptide.sub_sequence(self.start, self.stop, self.term, labeled=True))
        return self._lsequence
