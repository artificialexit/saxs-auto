#!/usr/bin/env python2

import os.path
import cStringIO
import math
from pprint import pprint

import logbook
logger = logbook.Logger(__name__)

          
def average(dat_list):
    result = DatFile()

    result.q = dat_list[0].q
    result.errors = dat_list[0].errors # this is wrong
    result.intensities = [ sum(row) / float(len(row)) for row in zip(*(dat.intensities for dat in dat_list)) ]
    
    # when we save we just want the rootname (as this is averaged)
    result.filename = dat_list[-1].rootname
    # another option could be (ie the name of the first frane used)
    #result.filename = data_list[0].basename
    return result

def subtract(dat_one, dat_two):    
    result = DatFile()
    
    result.q = dat_one.q
    result.errors = dat_one.errors # this is wrong    
    result.intensities = [ row[0] - row[1] for row in zip(dat_one.intensities, dat_two.intensities) ]
    
    result.filename = dat_one.basename
    return result


def rejection(dat_list):
    highq = (dat.highq for dat in dat_list)
    highthresh = 0.92 * max(highq)
    
    dat_list = [dat for dat in dat_list if dat.highq > highthresh]
    
    lowq = (dat.lowq for dat in dat_list)
    lowthresh = 1.08 * min(lowq)
    
    dat_list = [dat for dat in dat_list if dat.lowq < lowthresh]
    
    return dat_list
    
def compare(dat_one, dat_two):
    assert len(dat_one.q) == len(dat_two.q)
    assert dat_one.q == dat_two.q
    
    #for i in zip(dat_one.intensities, dat_two.intensities):
    #    print "%.10f %.10f" % i
    
    dat = subtract(dat_one, dat_two)
    diff = [ math.fabs(i) for i in dat.intensities ]
    print dat_one, dat_two
    print "Maximum difference %.10f" % max(diff)
    print "Minimum difference %.10f" % min(diff)
    print "Average difference %.10f" % (sum(diff)/float(len(diff)), )
    
    assert dat_one.intensities == dat_two.intensities
    
    print "PASS!!"
    
class DatFile(object):
    def __init__(self, datfile=None):
        self.filename = None
        
        self.q = []
        self.intensities = []
        self.errors = []

        self.valid = False
        
        if datfile:
            self.load(datfile)
    
    def load(self, filename):
        
        def yield_numbers(lines):
            for line in lines:
                try:
                    yield map(float, line)
                except ValueError:
                    pass
                
        with open(filename) as fp:
            # setup generator
            lines = (line.split() for line in fp)
            numbers = yield_numbers(lines)
            
            #unpack
            self.q, self.intensities, self.errors = zip(*(row for row in numbers))
            
        self.filename = filename
    
    def save(self, filename):
        
        f = cStringIO.StringIO()
        f.write("%s\n" % os.path.basename(filename))
        f.write("%14s %16s %16s\n" % ('q', 'I', 'Err'))
        for item in zip(self.q, self.intensities, self.errors):
            f.write("%18.10f %16.10f %16.10f\n" % item)
        
        with open(filename, 'w') as fp:
            fp.write(f.getvalue())
            
        self.filename = filename
        
    @property
    def rootname(self):
        return "".join((self.basename.rsplit('_', 1)[0], '.dat'))
    
    @property
    def basename(self):
        return os.path.basename(self.filename)
    
    @property
    def highq(self):
        return sum(self.intensities[-20:-1])
    
    @property
    def lowq(self):
        return sum(self.intensities[3:20])
    
    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.filename)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('files', nargs="+")
    args = parser.parse_args()
    
    dats = map(DatFile, args.files)
    compare(*dats)