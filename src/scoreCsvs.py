#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Given a pair of CSV files, generates another CSV containing the match score
# of values in the specified columns.

from difflib import SequenceMatcher
import argparse
import csv
import time

"""
    Class for comparing and scoring values in a pair of CSV tables.
    The value 'unknown' is especially scored as 0 if when both values do not match.
"""
class ScoreCsvs:

    def __init__(self, inputFileA, inputFileB, outputFile, debugFile):
        # Input CSV file A
        self.inputFileA = inputFileA
        # Input CSV file A
        self.inputFileB = inputFileB
        # Output CSV file
        self.outputFile = outputFile
        # Output CSV file
        self.debugFile = debugFile
        # Array with column IDs to score (ID is 0-based)
        self.checkColumns = None
        # Number of rows to parse
        self.rowsNum = 0
        print "Input A:", self.inputFileA
        print "Input B:", self.inputFileB
        print "Output:", self.outputFile
        print "Debug:", self.debugFile

    def parseColRange(self, rangeStr):
        result = set()
        for part in rangeStr.split(','):
            x = part.split('-')
            result.update(range(int(x[0]), int(x[-1])+1))
        self.checkColumns = sorted(result)
        if len(self.checkColumns) == 0:
            raise Exception("Must provide column IDs that are required to have data!")

    def setRows(self, num):
        self.rowsNum = int(num)

    def score(self):
        # Set up input file reader
        csvInputFileA = open(self.inputFileA, 'rb')
        csvInputFileReaderA = csv.reader(csvInputFileA, dialect='excel')
        csvInputFileB = open(self.inputFileB, 'rb')
        csvInputFileReaderB = csv.reader(csvInputFileB, dialect='excel')

        # Set up output files
        csvOutputFile = open(self.outputFile, 'wb')
        csvOutputWriter = csv.writer(csvOutputFile, dialect='excel')
        if self.debugFile:
            csvDebugFile = open(self.debugFile, 'wb')
            csvDebugWriter = csv.writer(csvDebugFile, dialect='excel')
            
        # Read the header line
        header = csvInputFileReaderA.next()
        header = csvInputFileReaderB.next()
        outHeader = []
        for j in self.checkColumns:
            outHeader.append(header[j])
        # Write header back
        csvOutputWriter.writerow(outHeader)
        sm = SequenceMatcher()

        for i in range(self.rowsNum):
            # A row from each file
            rowA = csvInputFileReaderA.next()
            rowB = csvInputFileReaderB.next()
            resp = []
            respDebug = []
            # For each column to be scored
            for j in self.checkColumns:
                if (rowA[j] != 'unknown' and rowB[j] != 'unknown'):
                    sm.set_seq1(rowA[j])
                    sm.set_seq2(rowB[j])
                    resp.append(sm.ratio())
                else:
                    if (rowA[j] == rowB[j]):
                        resp.append(1)
                    else:
                        resp.append(0)
                if self.debugFile: 
                    respDebug.append(rowA[j])
                    respDebug.append(rowB[j])
                    respDebug.append(resp[-1])

            # Write scores
            csvOutputWriter.writerow(resp)
            if self.debugFile: csvDebugWriter.writerow(respDebug)
        csvInputFileA.close()
        csvInputFileB.close()
        csvOutputFile.close()
        if self.debugFile: csvDebugFile.close()

"""
    Parses input arguments, constructs a ScoreCsv to score CSV files and
    output one without the scores.
    Example execution: python scoreCsvs.py -a ../data/CrowdsourcedData1.csv
        -b ../data/CrowdsourcedData2.csv -o ../data/UniqListOfCrowdValues.csv
        -c 6-8,16-22 -r 300
"""
def main():
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", required=True, help="Input csv file A")
    parser.add_argument("-b", required=True, help="Input csv file B")
    parser.add_argument("-c", required=True, help="Comma separated list of \
            columns IDs that are to be scored, ID is 0-based")
    parser.add_argument("-r", required=True, help="Number of rows to be scored")
    parser.add_argument("-o", "--output", required=True, help="Output csv file")
    parser.add_argument("-d", "--debugoutput", help="Debug Output csv file")
    args = parser.parse_args()

    # Instantiate and configure work csv cleaner
    startTime = time.time()
    sc = ScoreCsvs(args.a, args.b, args.output, args.debugoutput)
    sc.parseColRange(args.c)
    sc.setRows(args.r)
    sc.score()
    print "Done in ", time.time() - startTime, " secs"

if __name__ == "__main__":
    main()
