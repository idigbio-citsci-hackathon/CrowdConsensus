# -*- coding: utf-8 -*-
# Requires Python version 2.7
# Will not work on Python 3
from difflib import ndiff
from math import floor
import argparse
import csv
import collections
import os
import re
import time
import MyConfigParser

"""
    An aggregate map class. self.total represents the total number of
    items in the self.aggMap. self.aggMap is a map of string to the frequency
    these words were encountered. In the context of this problem, AggMap is
    used to group words that a similar. Only words who are in the majority
    AggMap will be considered as a solution to the consensus.
"""
class AggMap:
    def __init__(self):
        self.total = 0
        self.aggMap = collections.defaultdict(int)
    def __repr__(self):
        return str(self.total) + str(self.aggMap)

"""
    Class for calculating the consensus of crowdsourced inputs.
"""
class ConsensusCalculator:

    def __init__(self, inputFile, keyLabel, labelMap, labels):
        # Indicates if output is verbose for debugging
        self.debug = False

        # Input csv file
        self.inputFile = inputFile
        # Label that consensus entries are grouped by
        self.keyLabel = keyLabel
        # Output folder
        self.outputFolder = None
        # Indicates if lossless custom functions are to be used for normalizing data
        self.useFunctionNormalizer = False
        # Indicates if lossless translation tables are to be used for normalizing data
        self.useTranslationNormalizer = False
        # Indicates if voting is done as (top group/total entries) "False" or
        #                                (top group > second top group) "True"
        self.top2 = False
        # Indicates that all possible number of worker should be tried to
        # find the minimum number of workers needed for full consistency
        self.getMinWorker = False
        # Indicates if the work from a single worker should be output to the
        # consensus file.
        self.acceptBestWork = False
        
        # Maps the labels to the translation tables and dictionaries they will
        # need
        self.labelMap = labelMap
        # Labels to find a consensus for
        #self.labels = labelMap.keys()
        self.labels = labels

        # Output file names - TODO allow configuration?
        # File that shows the grouping of words
        self.groupingFileName = 'Grouping.csv'
        # Shows which value was chosen if a consensus was reached
        self.majorityFileName = 'Majority.csv'
        # Final consensus response achieved
        self.consensusFileName = 'Consensus.csv'
        # Shows which tasks did not achieve consensus when considering all the
        # fields in the row. Also determines if that tasks could have achieved
        # consensus with more or less workers.
        self.workerNeedFileName = 'WorkerNeed.csv'
        # Stores all changes performed to the original data by lossless transformations
        self.normalizedFileName = 'Normalized.csv'
        self.normalizedFile = None
        self.normalizedFileWriter = None

        # Diff output file name
        self.diffOutputFileName = 'Diff.txt'

        # The current translation table
        self.translationTable = None
        self.translationTables = None
        # The current spell correction dictionary
        self.correctionDictionary = None

        # String cleaner
        # Can be decorated for more functionality
        # Adds the PLSS cleaner by default 
        self.clean = self._noChange
        self.lossyClean = self._noChange
        # Exact String comparator - originally does only exact match
        # Can be decorated for more functionality
        self.comparator = self._exactMatch
        # Approximate string comparator
        self.approx = self._exactMatch

        # Punctuation to ignore
        # The first list removes general punctuation, the second removes punctuation if
        # not part of a number (ex: 20.99 -> 20.99, but other periods will be removed)
        self.puncPattern = re.compile(r"[\\,&\"(#)!?$:;'-]|([\./](?!\d))")

        # Flag to ignore empty strings
        self.ignoreEmptyStrings = False

        # Default responses to replace with self.defaultResponse
        self.ignoreDefaults = False
        self.defaultResponse = 'unknown'
        self.defaultResponsesToReplace = ['placeholder', 'unknown', 'Unknown',
                'not given', 'n/a', 'no data', 'nil', '']
        # The list of regex to detect unknown responses include: single lower
        # case letters and symbols, repeated characters (2 or more),
        # different types of unknowns (sn, na, none), only non-characters
        self.defaultRegexResponsesToReplace = ["^[-\\!?'\".a-z]$", "^([^0-9])\\1+$",\
            "^[uU]nknown .+$", "^[sS][. ]?[nN][.]?$", "^[nN][./ ]?[aA][./]?$",\
            "^[Nn][Oo][Nn][Ee][ ]?[\w?]*$", "^[-\\!?'\"., ]+$"]

        # Stopwords that can be ignored in a response
        self.stopwords = ['a', 'an', 'if', 'it', 'its', 'of', 'than',
                'that', 'the', 'to']

        # Define a regex to normal Public Lands Survey System Notation:
        # http://faculty.chemeketa.edu/afrank1/topo_maps/town_range.htm
        # define township match
        patternHead = '(?P<town>[T])(\.)?(\s)?(?P<townN>[0-9]+)(\s)?'
        patternHead += '(?P<townD>[NEWS])'
        patternHead += '|'
        #define the optional subsection match
        patternMid = '(?P<sub>(([NEWS./]{1,4})(\s)?((1/4)|(1/2)|Q,)(\s?)(of)?(\s?))+)'
        # define section match
        patternTail = '(?P<sec>Sec?)([.,])?(\s)?(?P<secN>[0-9]+)'
        patternTail += '|'
        # define range match
        patternTail += '(?P<range>R)(\.)?(\s)?(?P<rangeN>[0-9]+)(\s)?'
        patternTail += '(?P<rangeD>[NEWS])[.,;]?'
        # pattern WITH NO subsection match
        pattern = patternHead + patternTail
        # pattern WITH subsection match
        patternWSub = patternHead + patternMid + patternTail
        # define separator between occurrences
        pattern = '((' + pattern + ')(\s)?([.,;])?(\s)?)'
        patternWSub = '((' + patternWSub + ')(\s)?([.,;]?)?(\s)?)'
        # define min number of occurrences
        pattern = '(' + pattern + '{3,})(?P<close>([)]?))'
        patternWSub = '(' + patternWSub + '{3,})(?P<close>([)]?))'
        # Precompile the regex pattern for string with NO optional subsections
        self.plssRegex = re.compile(pattern, flags=re.IGNORECASE)
        self.plssReplace = '\\g<sec> \\g<secN>, '
        self.plssReplace += '\\g<town>\\g<townN>\\g<townD>, '
        self.plssReplace += '\\g<range>\\g<rangeN>\\g<rangeD>.'
        # Precompile the regex pattern for string WITH optional subsections
        self.plssRegexWSub = re.compile(patternWSub, flags=re.IGNORECASE)
        self.plssReplaceWSub = '\\g<sub> ' + self.plssReplace



    """
        Sets the output folder.
    """
    def setOutputFolder(self, outputFolder):
        self.outputFolder = outputFolder

    """
        Sets to use or not normalizers.
    """
    def setUseFunctionNormalizer(self, flag):
        self.useFunctionNormalizer = flag

    """
        Sets to use or not translation tables.
    """
    def setUseTranslationNormalizer(self, flag):
        self.useTranslationNormalizer = flag

    """
        Sets to use or not the voting strategy that selects the most voted group.
    """
    def setVoteTop2(self, flag):
        self.top2 = flag

    def setGetMinWorker(self, flag):
        self.getMinWorker = flag

    """
        Sets to accept or not the best answer when consensus is not achieved.
    """
    def setAcceptBestWork(self, flag):
        self.acceptBestWork = flag

    """     String Cleaners     """

    """
        Normalizes unknown cases.
    """
    def _normalizeUnknown(self, fn):
        def wrapped(string):
            # Call the other cleaning functions before (e.g., to trim whitespaces)
            normalized = fn(string)
            # Cleaning explicit unknown cases
            if normalized in self.defaultResponsesToReplace:
                normalized = self.defaultResponse
                if self.debug:
                    print "Normalizing to unknown fixed: [", string, "][", normalized, "]"
            else:
                for regex in self.defaultRegexResponsesToReplace:
                    if re.match(regex, normalized):
                        normalized = self.defaultResponse
                        if self.debug:
                            print "Normalizing to unknown: [", string, "][", normalized, "]"
                        break
            return normalized
        return wrapped

    """
        Normalizes string using self.translationTable
        It is assumed that the correct translation table will be set before
        attempting to normalize the string.
    """
    def translateString(self, label, string):
        normalized = string
        # Call the other cleaning functions before
        if self.translationTables.get(label):
            parts = string.split()
            for i in range(len(parts)):
                if parts[i]:
                    if parts[i][-1] == '.':
                        translated = self.translationTables.get(label).get(parts[i][:-1].lower(), parts[i])
                    elif parts[i][-1] in [',', ';', ')', ']']:
                        translated = self.translationTables.get(label).get(parts[i][:-1].lower(), parts[i][:-1]) + parts[i][-1]
                    else:
                        translated = self.translationTables.get(label).get(parts[i].lower(), parts[i])
                    if parts[i] != translated:
                        parts[i] = translated
            normalized = ' '.join(parts)
        return normalized

    """
        String cleaner wrapper - removes all unnecessary whitespace.
        Input is the function to wrap.
    """
    def _trimWhitespace(self, fn):
        def wrapped(string):
            return ' '.join(fn(string).split())
        return wrapped

    """
        Wraps the cleaning function with the whitespace cleaner. Also
        normalize unknown answers (including empty strings).
    """
    def addWhitespaceCleaner(self):
        self.clean = self._trimWhitespace(self._normalizeUnknown(self.clean))

    """
        String cleaner wrapper - returns a string with all lowercase.
        Input is the function to wrap.
    """
    def _caseInsensitive(self, fn):
        def wrapped(string):
            return fn(string).lower()
        return wrapped

    """
        Wraps the approximate function with the case insensitive cleaner.
    """
    def addCaseCleaner(self):
        self.lossyClean = self._caseInsensitive(self.lossyClean)

    """
        String cleaner wrapper - returns a string with punctuation removed.
        Input is the function to wrap.
    """
    def _ignorePunctuation(self, fn):
        def wrapped(string):
            # Remove punctuation
            return re.sub(self.puncPattern, '', fn(string))
        return wrapped

    """
        Wraps the approximate function with the punction cleaner.
    """
    def addPunctuationCleaner(self):
        self.lossyClean = self._ignorePunctuation(self.lossyClean)

    """
        Returns the original string - applies no change.
    """
    def _noChange(self, string):
        return string

    """     String Comparators  """

    """
        String align comparator - returns true if it is possible to align the
        two string such that the longer one contains the shorter one. string1
        is assumed to be the longer string.
    """
    def _wordAlignComparator(self, fn):
        def wrapped(string1, string2):
            tokens1 = sorted(re.findall(r"[\w']+", string1))
            tokens2 = sorted(re.findall(r"[\w']+", string2))
            pos1 = 0
            pos2 = 0
            while pos1 < len(tokens1) and pos2 < len(tokens2):
                if tokens1[pos1] == tokens2[pos2]:
                    pos1 += 1
                    pos2 += 1
                else:
                    pos1 += 1

            if pos2 == len(tokens2):
                return True
            else:
                return fn(string1, string2)
        return wrapped

    """
        String comparator - returns true if the second string is a substring of
        the first string. Input is the function to wrap. The first string must
        be the reference string.
    """
    def _substringComparator(self, fn):
        def wrapped(string1, string2):
            return (string2 in string1) or fn(string1, string2)
        return wrapped

    """
        Wraps the comparator function with the substring comparator and the
        word align comparator.
    """
    def addSubstringComparator(self):
        self.comparator = self._substringComparator(self.comparator)
        self.comparator = self._wordAlignComparator(self.comparator)

    """
        Approximate string comparator - returns true if the two strings have a Levenshtein
        distance of at most maxDist. Input fn is the function to wrap. Input
        maxDist is the max allowed Levenshtein distance that can return true.
    """
    def _levenshteinComparator(self, fn, maxDist):
        # Actual implementation of the Levenshtein distance function
        def _levDist(string1, string2):
            if len(string1) > len(string2):
                string1,string2 = string2,string1
            distances = range(len(string1) + 1)

            for index2,char2 in enumerate(string2):
                newDistances = [index2+1]
                for index1,char1 in enumerate(string1):
                    if char1 == char2:
                        newDistances.append(distances[index1])
                    else:
                        newDistances.append(1 + min(distances[index1],
                            distances[index1+1], newDistances[-1]))
                distances = newDistances
            return distances[-1]

        def wrapped(string1, string2):
            return (_levDist(string1, string2) <= maxDist) or fn(string1,string2)
        return wrapped

    """
        Wraps the approximate comparator function with the Levenshtein
        comparator.
    """
    def addLevenshteinComparator(self, maxDist):
        self.approx = self._levenshteinComparator(self.approx, maxDist)


    """
        String comparator - returns true if the sorted, concatenated words in
        the first string match the sorted, concatenated words in the second
        string.
    """
    def _fingerprintComparator(self, fn):
        def wrapped(string1, string2):
            s1 = ''.join(sorted(string1.split(' ')))
            s2 = ''.join(sorted(string2.split(' ')))
            return (s1 == s2) or fn(string1, string2)
        return wrapped


    """
        Wraps the approximate comparator function with the Fingerprint
        comparator.
    """
    def addFingerprintComparator(self):
        self.approx = self._fingerprintComparator(self.approx)


    """
        Approximate string comparator - returns true if the two strings are equal with ALL
        their whitespace removed. Input fn is the function to wrapped.
    """
    def _noWhitespaceComparator(self, fn):
        def wrapped(string1, string2):
            return ( ''.join(string1.split()) == ''.join(string2.split()) or fn(string1,string2) )
        return wrapped

    """
        Wraps the approximate comparator function with the no whitespace
        comparator.
    """
    def addWhitespaceIgnorer(self):
        self.approx = self._noWhitespaceComparator(self.approx)


    """
        Returns true if the two strings are equal. The first string must be the
        reference string.
    """
    def _exactMatch(self, string1, string2):
        return string1 == string2

    """     Main functions  """

    """
        Load configured translation tables. Assumes translation tables are in
        the same folder as the configuration file.
    """
    def loadTranslationTable(self, folder):
        if folder:
            cfgFolder = folder + "/"
        self.translationTables = collections.defaultdict()
        for label in self.labels:
            data = collections.defaultdict()
            tables = self.labelMap[label].trans
            for table in tables:
                for line in open(cfgFolder + table):
                    words = line.split(':')
                    data[words[0].strip()] = words[1].strip()
            self.translationTables[label] = data

    """
        Helper method to flatten a list of AggMaps.
        Returns a list in the following format:
        [[Aggmap1's k,v], [Aggmap2's k,v], ... ]
    """
    def _flattenAggMapList(self, l):
        ret = []
        # Traverse of AggMaps
        for m in l:
            t = []
            # Traverse over key's and values in that AggMap
            for k,v in m.aggMap.items():
                t.append((k,v))
            ret.append(t)
        return ret

    """
        Returns the grouping file header.
    """
    def getGroupingHeader(self):
        row = [self.keyLabel]
        for label in self.labels:
            row.append(label)
        return row

    """
        Returns the consensus file header.
    """
    def getConsensusHeader(self):
        row = [self.keyLabel]
        for label in self.labels:
            row.append(label)
        return row

    """
        Returns the majority file header.
    """
    def getMajorHeader(self):
        row = [self.keyLabel]
        for label in self.labels:
            row.append(label)
            # Options for the label's consensus
            row.append('Options')
            # The max vote for workers within the label
            row.append('Max')
            # The total number of votes
            row.append('Out of')
            # Max / Total
            row.append('Ratio')
            # 'ok' if the label reached a majority
            row.append('Majority')
            # Lossy or lossless
            row.append('Algorithm Type')
            # Consensus response type (real / blank) if consensus is reached
            row.append('Consensus Response Type')
        # 'ok' if all the labels for the task reached consensus
        row.append("Complete Consensus")
        return row

    def countUpper(self, string):
        return sum(c.isupper() for c in string)

    def countLower(self, string):
        return sum(c.islower() for c in string)

    def countVowel(self, string):
        return sum(1 if c in list('aeiouAEIOU') else 0 for c in string)

    def properName(self, string):
        if len(string) == 0 or string == self.defaultResponse:
            return string
        copy = re.sub('[.]', '. ', string)
        copy = re.sub('[,]', ', ', copy)
        copy = re.sub('[&]', ' and ', copy)
        copy = re.sub('[+]', ' and ', copy)
        copy = re.sub('[wW][ ]?[/]', ' with ', copy)
        remainLower = ['and', 'with', 'aff.', 'comb.', 'ex', 'ex.', 'ined.', 'nov.', 'var.', 'sp.', 'vel']
        parts = copy.split(' ')
        resp = []
        for part in parts:
            if len(part) == 1:
                if (part == ',' or part == ')' or part == '.') and len(resp) > 0:
                    # If it is a single comma, parenthesis or dot -> concatenate with previous word
                    resp[-1] = resp[-1] + part
                elif part.isalpha():
                    # Assume 1 letter is an abbreviation
                    resp.append(part.upper() + '.')
                else:
                    resp.append(part)
            elif part.lower() in remainLower:
                resp.append(part.lower())
            elif len(part) > 1:
                uChars = self.countUpper(part)
                lChars = self.countLower(part)
                vChars = self.countVowel(part)
                if (uChars > 1 and lChars > 1):
                    # Upper and lower case mixed or all up to 3 upper -> keep original
                    resp.append(part)
                elif (uChars == 2 and lChars == 0 and len(part) == 2 and part != "JR"):
                    # All 2 upper -> assume it is an abbreviation (except for Jr)
                    resp.append(". ".join(list(part)) + ".")
                elif (uChars == 2 and lChars == 0 and len(part) == 3 and part[0:2] != "JR" and part[0:2] != "TO"):
                    # All 2 upper with a symbol -> assume it is an abbreviation (except for Jr)
                    resp.append(re.sub('[ ][,]', ',', re.sub('[ ][.]', '', re.sub('([A-Z])', '\\1. ', part))).rstrip())
                elif (uChars == 3 and lChars == 0 and len(part) < 5 and (part != "JNR" or part != "JNR.") and vChars == 0):
                    # All 3 upper without vowels -> assume it is an abbreviation (except for Jr)
                    resp.append(re.sub('[ ][,]', ',', re.sub('[ ][.]', '', re.sub('([A-Z])', '\\1. ', part))).rstrip())
                else:
                    # In all other cases -> capitalize only first letter
                    # Cannot use title accented characters are counted as delimiters Löve -> LöVe
                    for i, c in enumerate(part):
                        if c.isalpha():
                            break
                    resp.append(part[:i] + part[i:].capitalize())
        norm = ' '.join(resp)
        if self.debug:
            if string != norm: print "[" + string + "]->[" + norm + "]"
        return norm

    """
        Normalizes the Public Lands Survey System Notation.
    """
    def normalizePLSS(self, string):
        # If the string is malformed, do not attempt to normalize the PLSS
        # A malformed string (ex: 'NW1/4 of NE1/4 Sec 35, T2S, T21W' will
        # match the regex, but not match all the groups in the regex.
        # See if string contains PLSS with the recursive section
        # subdivision
        norm = string.replace('¼', '1/4')
        try:
            norm = re.sub(self.plssRegexWSub, lambda pat: pat.group('sub').upper().replace('S/','S').replace('N/','N').replace('Q,','1/4').replace('OF ', '').replace('.','').replace(' 1', '1').strip() +
                " Sec. " + pat.group('secN') +
                ', T' + pat.group('townN') + pat.group('townD').upper() +
                ', R' + pat.group('rangeN') + pat.group('rangeD').upper() + (')' if pat.group('close') else '. '), string)
            if self.debug:
                if norm != string: print '[\n' + string + '\n' + norm + '\n]\n'
            if norm != string: return ' '.join(norm.split())
        except:
            None
        # See if string contains PLSS without the recursive section subdivision
        try:
            norm = re.sub(self.plssRegex, lambda pat: "Sec. " + pat.group('secN') +
                ', T' + pat.group('townN') + pat.group('townD').upper() +
                ', R' + pat.group('rangeN') + pat.group('rangeD').upper() + (')' if pat.group('close') else '. '), string)
            if self.debug:
                if norm != string: print '{{\n' + string + '\n' + norm + '\n}}\n'
            return ' '.join(norm.split())
        # String does not contain PLSS
        except:
            return norm
        
    """
        Normalizes the Public Lands Survey System Notation.
    """
    def normalizeLatLong(self, string):
        # Normalize latitude followed by longitude in decimal, with labels lat and long
        subn = re.subn('(?i)(?P<ll>(Lat|Long)(itude)?[.:]?)\s?(?P<num>([0-9]+([.][0-9]+)))(\s)*(?P<dir>([NEWS]))',
            lambda pat: ('Latitude: ' if 'lat' in pat.group('ll').lower() else 'Longitude: ') + 
            pat.group('num') + pat.group('dir').upper(), string)
        if subn[1] > 0:
            norm = subn[0]
        else:
            # Normalize latitude followed by longitude in decimal
            norm = re.sub('(?i)(?P<numA>([0-9]+([.][0-9]+)))(\s)*(?P<dirA>([NS]))[,;,]\s?(?P<numB>([0-9]+([.][0-9]+)))(\s)*(?P<dirB>([EW]))',
                lambda pat: 'Latitude: ' + pat.group('numA') + pat.group('dirA') +
                ' Longitude: ' + pat.group('numB') + pat.group('dirB'), string)
            # Normalize latitude in degrees
            norm = re.sub('(?i)(?P<deg>[0-9]+)(deg|[°o* ])(\s)*(?P<min>[0-9]+)([\'])(\s)*(?P<sec>[0-9]+)([\'][\' ]?)(?P<dir>[NS])( lat)?',
                lambda pat: 'Latitude: ' + pat.group('min') + '°' + pat.group('min') + '\'' + pat.group('sec') +
                '"' + pat.group('dir'), norm)
            # Normalize longitude in degrees
            norm = re.sub('(?i)(?P<deg>[0-9]+)(deg|[°o* ])(\s)*(?P<min>[0-9]+)([\'])(\s)*(?P<sec>[0-9]+)([\'][\' ]?)(?P<dir>[EW])( long| lon)?',
                lambda pat: 'Longitude: ' + pat.group('min') + '°' + pat.group('min') + '\'' + pat.group('sec') +
                '"' + pat.group('dir'), norm)
            if self.debug:
                if norm != string: print '[\n' + string + '\n' + norm + '\n]\n'
        return norm

    """
        Returns the sorted and blank-normalized list of attributes and a
        copy (with indices)
    """
    def _getSortedAttrsForLabel(self, attrSets, label):
        sortedAttrs = [attrSet[label] for attrSet in attrSets]

        # Normalize any default responses
        sortedAttrsTemp = []
        for attr in sortedAttrs:
            normalized = self.clean(attr)
            # Apply all lossless functions for this attribute
            if self.useFunctionNormalizer:
                losslessFuncs = self.labelMap[label].funcs
                for func in losslessFuncs:
                    normalized = getattr(self, func)(normalized)

            # Apply all lossless translation tables for this attribute
            if self.useTranslationNormalizer:
                normalized = self.translateString(label, normalized)

            if attr != normalized and (self.useFunctionNormalizer or self.useTranslationNormalizer):
                self.normalizedFileWriter.writerow(["orig:", attr])
                self.normalizedFileWriter.writerow(["norm:", normalized])
                self.normalizedFile.flush()

            # Using the cleaned version of the data
            sortedAttrsTemp.append(normalized)

        # Copy back the normalized version
        sortedAttrs = sortedAttrsTemp

        # Sort attributes in order of descending length
        sortedAttrs.sort(key=len, reverse=True)

        # Create a copy of the original attributes to use when determining
        # whether to add more or less workers to a task. Each attribute
        # will also be match with is corresponding original indice in attrSets
        indices = range(len(sortedAttrs))
        sortedAttrsCopy = sortedAttrs[:]
        sortedAttrsCopy = zip(sortedAttrsCopy, indices)

        return sortedAttrs, sortedAttrsCopy

    """
        List of grouped words and their cumulative frequencies. In the
        end, the group with the largest cumulative frequency will have
        the majority selected from it. The idea is that only similar
        words will be located in the same group.
        group.total = cumulative weight of this group
        group.aggMap = dictionary of key to weight

        If useLossyNormalizers = True, then lossy normalizers will be used as
        well in the comparision. Else, only lossless normalizers will be used.
    """
    def _buildFrequencyList(self, sortedAttrs, useLossyNormalizers):
        freqList = []
        # Loop over each attribute to try to put it in a group
        for sortedAttr in sortedAttrs:
            attrFound = False
            # Look over all groups
            for group in freqList:
                for key in group.aggMap.keys():
                    # Insert exactly
                    if self.comparator(key, sortedAttr):
                        group.aggMap[key] += 1
                        group.total += 1
                        attrFound = True
                        break
                    # Try approximate match needed
                    if useLossyNormalizers:
                        s1 = self.lossyClean(key)
                        s2 = self.lossyClean(sortedAttr)
                        if (self.approx(s1,s2) or self.comparator(s1,s2)):
                            group.aggMap[sortedAttr] += 1
                            group.aggMap[key] += 1
                            group.total += 1
                            attrFound = True
                            break
                if attrFound:
                    break
            # If no exact or approximate matches, create a new entry
            if not attrFound:
                newEntry = AggMap()
                newEntry.aggMap[sortedAttr] += 1
                newEntry.total += 1
                freqList.append(newEntry)
        return freqList

    """
        Returns the majority entry(ies), the votes for the entry(ies), and all
        the keys that were in the majority group containing that entry. The
        keys that were in the majority group containing the majority entry
        essentially 'voted' for the majority entry.
    """
    def _majorityFromFrequencyList(self, freqList):
        # Search for the majority entry
        maxVotes = -1
        maxVotes2 = -1
        majorGroup = []
        # Find the majority group(s)
        for group in freqList:
            if (group.total == maxVotes):
                maxVotes2 = maxVotes
                majorGroup.append(group.aggMap)
            elif (group.total > maxVotes):
                majorGroup = [group.aggMap]
                maxVotes2 = maxVotes
                maxVotes = group.total
        # Search for the majority value in the majority group
        # Assumption is the similar entries in the same group are
        # mispellings, but their vote counts towards the biggest entry
        currMax = -1
        majorEntry = []
        for group in majorGroup:
            unsortKeys = group.keys()
            unsortKeys.sort(key = lambda s: -len(s))
            for key in unsortKeys:
                if group[key] == currMax:
                    majorEntry.append(key)
                elif group[key] > currMax:
                    majorEntry = [key]
                    currMax = group[key]

        # All entries that were in the majority entry. Usually only one,
        # but can be multiple if a consensus was not reached
        majorGroupKey = []
        for group in majorGroup:
            for key in group.keys():
                majorGroupKey.append(key)
        return majorEntry, maxVotes, maxVotes2, majorGroupKey


    """
        Writes the diffs of the freqList into a output file.
    """
    def _writeDiffToFile(self, fileKey, label, freqList):
        if not self.debug:
            return
        diffOutput = []
        # Output diff between entries
        diffOutput.append('========\n')
        diffOutput.append(label + ' (' + fileKey + '):\n')

        groups = []
        # Traverse the AggMaps
        # Extract the keys, but keep their grouping
        # Also output the grouping of the keys to the output file
        for groupMap in freqList:
            tempList = []
            diffOutput.append('(\n')
            for key in groupMap.aggMap.keys():
                tempList.append(key)
                diffOutput.append('\t' + key + '\n')
            diffOutput.append(')\n')
            groups.append(tempList)
        diffOutput.append('========\n')

        # Compare only keys in different groups to each other
        # First group
        for i in range(len(groups)):
            # Items in the first group
            for k in range(len(groups[i])):
                # Second group
                for j in range(len(groups)-i-1):
                    # Items in the second group
                    for h in range(len(groups[i+j+1])):
                        # Write lossless clean first
                        s1 = (self.clean((groups[i])[k]) + '\n').splitlines(1)
                        s2 = (self.clean((groups[i+j+1])[h]) + '\n').splitlines(1)
                        diff = ndiff(s1, s2)
                        diffOutput.append('[LOSSLESS]\n')
                        diffOutput.append(''.join(diff))
                        # Then write lossy clean
                        s1 = (self.lossyClean(self.clean((groups[i])[k])) + '\n').splitlines(1)
                        s2 = (self.lossyClean(self.clean((groups[i+j+1])[h])) + '\n').splitlines(1)
                        diff = ndiff(s1, s2)
                        diffOutput.append('[LOSSY]\n')
                        diffOutput.append(''.join(diff))
                        diffOutput.append('~~~\n')
        diffWriter = open(os.path.join(self.outputFolder, self.diffOutputFileName), 'a')
        diffWriter.writelines(diffOutput)
        diffWriter.close()


    """
        Returns the majority row, statistics row, and a worker need row for the
        attribute value corresponding to the given file key. The workerNeedRow may
        be None if the row reached a consensus when considering all the fields.

        attrSets is a list of maps from string to string, where the key is the
        label and the value is the value associated with the label.

        Return value is (majorityRow, groupingRow, consensusRow, workerNeedRow)
    """
    def getConsensus(self, fileKey, attrSets):
        # The majority statistics, grouping, and consensus rows to be returned
        majorityRow = [fileKey]
        groupingRow = [fileKey]
        consensusRow = [fileKey]
        # The worker need row to be returned
        workerNeedRow = None

        # For the workerNeedRow
        # The total number of labels that have achieved consensus
        totalOk = 0
        # The set of entries that can be ignored (removed from the input) and
        # have this task still reach an overall consensus
        entriesNotUsedForMajority = set(i for i in range(len(attrSets)))
        # The max number of votes a label is lacking to achieve consensus
        # (ex if 4 votes were received and 7 were needed,
        # maxVotesNeededForMajority = max(3, maxVotesNeededForMajority)
        maxVotesNeededForMajority = 0
        # This represents the set of tasks that all entries had in common that
        # contributed to the majority. A subset of the intersection of
        # entriesUsedForMajority and entriesNotUsedForMajority that is at most
        # size minVotesNeededToKeepMajority can be removed for the tasks
        # without affecting the results
        entriesUsedForMajority = set(i for i in range(len(attrSets)))
        # This number represents the smallest number of votes a label had above
        # the majority
        minVotesNeededToKeepMajority = len(attrSets)

        # Iterate over the labels to to find a consensus per label 
        for label in self.labels:
            # Indicates if lossy normalizer is used for a particular label
            useLossyNormalizers = False
            # Indicates if consensus is reached for a particular label
            consensusFound = False

            # Get the sorted attributes for this label from the original 
            # attribute set
            sortedAttrs, sortedAttrsCopy = self._getSortedAttrsForLabel(attrSets, label)

            # Two iterations to find consensus: the first without the lossy
            # normalizers and the second with the lossy normalizers. All labels
            # will go through the first iteration, but only those that do not reach
            # consensus in the first iteration will move to the second iteration.
            for consensusIteration in range (2):
                # Break early if we found a consensus the first found (using
                # lossless normalizers)
                if consensusFound:
                    break

                # Use lossy normalizers during the second iteration
                if consensusIteration == 1:
                    useLossyNormalizers = True

                # List of grouped words and their cumulative frequencies. In the
                # end, the group with the largest cumulative frequency will have
                # the majority selected from it. The idea is that only similar
                # words will be located in the same group.
                # group.total = cumulative weight of this group
                # group.aggMap = dictionary of key to weight
                freqList = self._buildFrequencyList(sortedAttrs, useLossyNormalizers)

                # Get the majority entry(ies), the votes for that entry(ies),
                # as well the entries that voted for the majority entry(ies).
                majorEntry, maxVotes, maxVotes2, majorGroupKey = self._majorityFromFrequencyList(freqList)
                totalVotes = len(sortedAttrs)

                # The entries not in the majority group
                ignoredEntries = set(indice for value,indice in
                        sortedAttrsCopy if value not in
                        majorGroupKey)
                # The entries in the majority group
                usedEntries = set(indice for value,indice in
                        sortedAttrsCopy if value in
                        majorGroupKey)

                votesForMajority = floor(0.5 * totalVotes) + 1

                if (totalVotes > 1):
                    # It only makes sense to calsulate ratio if there is more
                    # than 1 worker's answer
                    if self.top2:
                        if (maxVotes2 != -1):
                            ratio = maxVotes / float(maxVotes + maxVotes2)
                        else:
                            ratio = 1
                    else:
                        ratio = maxVotes / float(totalVotes)
                else:
                    ratio = 0

                # A label that reached majority will be processed here only.
                if ratio > 0.5:
                    consensusFound = True

                    # Format grouping row
                    groupingRow.append(self._flattenAggMapList(freqList))

                    # Format consensus row
                    consensusRow.append(majorEntry[0])

                    # Format this majority row - see self.getMajorHeader()
                    # Majority corresponding to label
                    majorityRow.append(majorEntry)
                    # Options
                    majorityRow.append(len(majorEntry))
                    # Max (max votes)
                    majorityRow.append(maxVotes)
                    # Out of
                    majorityRow.append(totalVotes)
                    # Ratio
                    majorityRow.append(ratio)
                    # Label Consensus
                    majorityRow.append('ok')
                    # Algorithm type
                    if useLossyNormalizers:
                        majorityRow.append('lossy')
                    else:
                        majorityRow.append('lossless')
                    # Consensus Response Type
                    # Real
                    if len(majorEntry) == 1 and majorEntry[0] != self.defaultResponse:
                        majorityRow.append('real')
                    # Blank / Default response
                    elif len(majorEntry) == 1 and majorEntry[0] == self.defaultResponse:
                        majorityRow.append('blank')
                    # More than one majority response
                    else:
                        if self.defaultResponse in majorEntry:
                            # This really should not happen
                            majorityRow.append('blank2')
                        else:
                            majorityRow.append('real')
                        self._writeDiffToFile(fileKey, label, freqList)

                    totalOk += 1
                    votesExtra = maxVotes - votesForMajority
                    # Calculate the min number of votes that can be removed from
                    # the majority and still allow the majority to be kept
                    minVotesNeededToKeepMajority = min(
                            minVotesNeededToKeepMajority, votesExtra)
                    # Update the set of entries used for calculating a majority to
                    # be the intersection of entries that have been used so far and
                    # the set of entries that were were to calculate the majority
                    # for this attribute
                    entriesUsedForMajority &= usedEntries
                # Only process labels that did not reach a majority if we are
                # in the consensus iteration using lossy normalizers.
                # In other words, anything here did not reach a consensus!
                elif useLossyNormalizers:
                    # Format grouping row
                    groupingRow.append(self._flattenAggMapList(freqList))

                    # Format consensus row
                    if self.acceptBestWork:
                        # Output best choice even when majority was not reached
                        consensusRow.append(majorEntry[0])
                    else:
                        consensusRow.append('')

                    # Format this majority row - see self.getMajorHeader()
                    # Majority corresponding to label
                    majorityRow.append(majorEntry)
                    # Options
                    majorityRow.append(len(majorEntry))
                    # Max (max votes)
                    majorityRow.append(maxVotes)
                    # Out of
                    majorityRow.append(totalVotes)
                    # Ratio
                    majorityRow.append(ratio)
                    # Label Consensus
                    majorityRow.append('')
                    # Algorithm Type
                    majorityRow.append('')
                    # Consensus Response Type, empty since majority was not reached
                    majorityRow.append('')

                    # Output the key diffs if this label did not reach a consensus
                    self._writeDiffToFile(fileKey, label, freqList)

                    # How many votes are still needed for a majority?
                    votesNeeded = votesForMajority - maxVotes
                    # If there are 2+ major entries, then we might need more
                    # workers for this task
                    if len(majorEntry) == 1:
                        # Calculate the max number of votes needed for all
                        # attributes to attain the majority
                        maxVotesNeededForMajority = max(votesNeeded,
                                maxVotesNeededForMajority)
                        # Update the set of ignored entries to be the intersection
                        # of the set of entries that have been ignored so far and
                        # the set of entries that were ignored when calculating the
                        # consensus for this attribute
                        entriesNotUsedForMajority &= ignoredEntries
                    # This ensures that the task will be assigned more workers
                    else:
                        maxVotesNeededForMajority = len(attrSets)

        # Before returning, create the worker need row if this task did not reach a
        # satisfactory consensus when considering all fields.
        # Satisfactory consensus: All labels must reach consensus, there must
        # be a least 2 workers for this task

        # Entries that can be removed and still keep the consensus
        entriesThatCanBeRemoved = entriesUsedForMajority & entriesNotUsedForMajority

        # The task did not reach complete consensus 
        if totalOk != len(self.labels) or len(attrSets) == 1:
            majorityRow.append('')
            workerNeedRow = [fileKey]
            workerNeedRow.append(totalOk)
            # Only one worker for this task
            if len(attrSets) == 1:
                workerNeedRow.append('More')
                workerNeedRow.append('Only 1 worker assigned to task')
            # Should not take away a worker if there are only two workers
            elif len(attrSets) == 2:
                workerNeedRow.append('More')
                workerNeedRow.append('Need one more worker for consensus')
            # If the max votes needed for a group to reach a complete majority
            # and the max votes is also less than the number of allowable
            # entries to remove, then entries can removed to allow the task to
            # achieve complete consensus
            elif maxVotesNeededForMajority <= minVotesNeededToKeepMajority and maxVotesNeededForMajority <= len(entriesThatCanBeRemoved):
                reason = 'Can remove ' + str(int(maxVotesNeededForMajority))
                reason += ' worker(s) (' + str(len(attrSets))
                reason += ' total) from ('
                for i in entriesThatCanBeRemoved:
                    reason += str(i) + ' '
                reason += ') to reach full consensus'
                workerNeedRow.append('Less')
                workerNeedRow.append(reason)
            # By default assign more workers to a task
            else:
                workerNeedRow.append('More')
                workerNeedRow.append('Default')
        # The task did reach complete consensus
        else:
            majorityRow.append('ok')

        return (majorityRow, groupingRow, consensusRow, workerNeedRow)


    """
        Reads the input file, computes the grouping and majority, and writes the
        results to the output files.
        Should be called after setting any optional parameters for the
        ConsensusCalculator.
    """
    def calculateConsensus(self):
        # Set up input file reader
        csvInputFile = open(self.inputFile, 'rb')
        csvInputFileReader = csv.DictReader(csvInputFile, dialect='excel')

        if not self.outputFolder:
            self.outputFolder = os.path.dirname(self.inputFile)
        if not os.path.exists(self.outputFolder):
            os.makedirs(self.outputFolder)
        
        # Set up output files
        groupingFile = open(os.path.join(self.outputFolder, self.groupingFileName), 'wb')
        groupingFileWriter = csv.writer(groupingFile, dialect='excel')
        majorityFile = open(os.path.join(self.outputFolder, self.majorityFileName), 'wb')
        majorityFileWriter = csv.writer(majorityFile, dialect='excel')
        consensusFile = open(os.path.join(self.outputFolder, self.consensusFileName), 'wb')
        consensusFileWriter = csv.writer(consensusFile, dialect='excel')
        self.normalizedFile = open(os.path.join(self.outputFolder, self.normalizedFileName), 'wb')
        self.normalizedFileWriter = csv.writer(self.normalizedFile, dialect='excel')
        # Output for tasks that did not reach a majority considering all fields
        workerNeedFile = open(os.path.join(self.outputFolder, self.workerNeedFileName), 'wb')
        workerNeedFileWriter = csv.writer(workerNeedFile, dialect='excel')


        print "Creating file attribute map..."
        # Create the file attribute map
        # Maps filenames to the rows associated with them
        fileMap = collections.defaultdict(list)
        # Each row is a dictionary of strings to strings
        # Keys are the names of the columns (skipped by the reader)
        # Values are data from the row being read
        for row in csvInputFileReader:
            fileMap[ row[self.keyLabel] ].append(row)

        # Write output file headers
        groupingFileWriter.writerow( self.getGroupingHeader() )
        groupingFile.flush()
        majorityFileWriter.writerow( self.getMajorHeader() )
        majorityFile.flush()
        consensusFileWriter.writerow( self.getConsensusHeader() )
        consensusFile.flush()
        workerNeedFileWriter.writerow(['Total # of attributes ', str(len(self.labels))])
        workerNeedFileWriter.writerow(['Name', '# attributes reaching consensus', 'More/Less workers needed', 'Reason'])


        print "Determining consensus among data..."
        # Create and write consensus data
        # Data written to output is not sorted by keyLabel 
        numRowsProcessed = 0
        totalWithoutFullConsensus = 0
        startTime = time.time()
        for key in sorted(fileMap.keys()):
            if self.getMinWorker:
                majorityRow = None
                minW = 0
                for i in range(1,len(fileMap[key]) + 1):
                    majorityRow, groupingRow, consensusRow, workerNeedRow = self.getConsensus(key, fileMap[key][:i])
                    minW = i
                    if (majorityRow[-1] == 'ok'):
                        break
                if majorityRow:
                    groupingRow.append('Worker reduceable from/to:')
                    groupingRow.append(len(fileMap[key]))
                    groupingRow.append(minW)
                else:
                    continue
            else:
                majorityRow, groupingRow, consensusRow, workerNeedRow = self.getConsensus(key, fileMap[key])
            numRowsProcessed += 1
            groupingFileWriter.writerow(groupingRow)
            groupingFile.flush()
            majorityFileWriter.writerow(majorityRow)
            majorityFile.flush()
            consensusFileWriter.writerow(consensusRow)
            consensusFile.flush()
            # Write the workerNeed row if necessary
            if workerNeedRow:
                totalWithoutFullConsensus += 1
                workerNeedFileWriter.writerow(workerNeedRow)
                workerNeedFile.flush()

            # Print some output for calculations that take a long time
            if (numRowsProcessed % 100 == 0):
                stopTime = time.time()
                print "Completed", numRowsProcessed, "out of", len(fileMap), \
                    "in", stopTime - startTime, " secs"
                startTime = stopTime

        # Footer for the workerNeed file
        workerNeedFileWriter.writerow(
            ['% of tasks that did not reach a satisfactory consensus: ',
            str((100.0*totalWithoutFullConsensus)/numRowsProcessed)])

        # Close files
        csvInputFile.close()
        groupingFile.close()
        majorityFile.close()
        consensusFile.close()
        self.normalizedFile.close()
        workerNeedFile.close()

"""
    Parses input arguments, constructs a ConsensusCalculator, and evaluates the
    data if possible.
"""
def main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", required=True, help="Input csv file to evaluate")
    parser.add_argument("-g", required=True, help="Configuration file")
    parser.add_argument("-b", help="Trim/clean extra spaces and normalize \
            empty responses", action="store_true")
    parser.add_argument("-n", help="Normalize using transformation functions \
            (ex properName)", action="store_true")
    parser.add_argument("-t", help="Normalize using translation tables \
            (ex east -> e)", action="store_true")
    parser.add_argument("-w", help="Approximate matching by ignoring ALL \
            whitespace", action="store_true")
    parser.add_argument("-c", help="Case insensitive comparison",
            action="store_true")
    parser.add_argument("-s", help="Use substring matching",
            action="store_true")
    parser.add_argument("-p", help="Ignore punctuation",
            action="store_true")
    parser.add_argument("-f", help="Fingerprint comparison", action="store_true")
    parser.add_argument("-l", help="Use Levenshtein distance matching - Arg \
            is max allowed distance", type=int)
    parser.add_argument("-v", help="Majority voting based on top 2 group counts",
            action="store_true")
    parser.add_argument("-a", help="Accept the best crowdsourcing work even if \
            consensus was not reached. This is useful for creating the golden response.",
            action="store_true")
    parser.add_argument("-o", "--output", help="Output folder")
    parser.add_argument("-m", help="Calculate minimum number of workers needed \
            for full consistency. In this case, single worker tasks are skipped.",
            action="store_true")
    args = parser.parse_args()


    # Read in the config file
    # Returns a map of label names to LabelInfo objects
    parsedConfig = MyConfigParser.parseConfig(args.g)

    print "Using key: ", parsedConfig.keyLabel
    print "Using labels:", parsedConfig.orderedLabels
    
    # Configure consensus calculator
    calculator = ConsensusCalculator(args.i, parsedConfig.keyLabel,
            parsedConfig.labelMap, parsedConfig.orderedLabels)

    if args.output:
        calculator.setOutputFolder(args.output)
    print "Output to folder: ", calculator.outputFolder

    # Lossless cleaners
    if args.b:
        calculator.addWhitespaceCleaner()

    # Lossless custom per attribute function normalizer
    if args.n:
        calculator.setUseFunctionNormalizer(True)

    # Lossless custom per attribute translation table normalizer
    if args.t:
        calculator.setUseTranslationNormalizer(True)
        calculator.loadTranslationTable(parsedConfig.configFolder)

    # Lossy cleaners/Approximate comparator
    if args.w:
        calculator.addWhitespaceIgnorer()

    if args.c:
        calculator.addCaseCleaner()

    if args.s:
        calculator.addSubstringComparator()

    if args.p:
        calculator.addPunctuationCleaner()

    if args.f:
        calculator.addFingerprintComparator()

    if args.l:
        calculator.addLevenshteinComparator(args.l)

    # Change voting function
    if args.v:
        calculator.setVoteTop2(True)

    # Change consensus output
    if args.a:
        calculator.setAcceptBestWork(True)

    if args.m:
        calculator.setGetMinWorker(True)

    # Evaluate file
    calculator.calculateConsensus()


if __name__ == "__main__":
    main()
