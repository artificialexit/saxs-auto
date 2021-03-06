#!/usr/bin/env python2

import os.path
import time
import cStringIO
import math
from pprint import pprint

import logbook
logger = logbook.Logger(__name__)

try:
    import numpy as np
except:
    print "No numpy, some functions wont work"

          
def average(dat_list, weight_list = []):
    
    result = DatFile()

    if dat_list == None:
        return result
    
    if len(dat_list) == 0:
        return result

    if len(weight_list) == 0:
        weight_list = [1.0]*len(dat_list)

    weight_list = [len(weight_list)*weight/float(sum(weight_list)) for weight in weight_list]

    result.q = dat_list[0].q
    result.errors = [ math.sqrt(sum(el**2 for el in row))/ float(len(row)) for row in zip(*([[e*w for e in dat.errors]
                                                                           for dat, w in zip(dat_list, weight_list)]))]

    result.intensities = [ sum(row) / float(len(row)) for row in zip(*([[i*w for i in dat.intensities]
                                                      for dat, w in zip(dat_list, weight_list)]))]
    
    # when we save we just want the rootname and index of first and last to create unique names. (as this is averaged)
    result.filename = os.path.join(dat_list[-1].dirname, dat_list[0].basename)
    result.setheader('Used file indices: %s' % (' '.join(d.fileindex for d in dat_list),))
    for dat in dat_list:
        result.setuserdata(dat.userData)
    # another option could be (ie the name of the first frane used)
    #result.filename = data_list[0].basename
    return result

def subtract(dat_one, dat_two):    
    result = DatFile()
    result.userData = dat_one.userData
    
    result.q = dat_one.q
    result.errors = [ math.sqrt(sum(el**2 for el in row))/ float(len(row)) for row in zip(dat_one.errors, dat_two.errors) ]
    result.intensities = [ row[0] - row[1] for row in zip(dat_one.intensities, dat_two.intensities) ]
   
    result.filename = os.path.join(dat_one.dirname,dat_one.basename)
    return result


def rejection(dat_list):
    highq = (dat.highq for dat in dat_list)
    highthresh = 0.92 * max(highq)
    
    if highthresh == 0:
        return

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
        
        self.header = ''
        
        self.q = []
        self.intensities = []
        self.errors = []

        self.valid = False
        
        self.userData = {}
        
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
    
    def save(self, filename, header = ''):
        
        if header != '':
            self.setheader(header)

        f = cStringIO.StringIO()
        f.write("%s; %s\n" % (os.path.basename(filename),self.header))
        f.write("%14s %16s %16s\n" % ('q', 'I', 'Err'))
        for item in zip(self.q, self.intensities, self.errors):
            f.write("%18.10f %16.10f %16.10f\n" % item)
        
        with open(filename, 'w') as fp:
            fp.write(f.getvalue())
            
        self.filename = filename
    
    def setheader(self, header, replace=False):
        if replace==True:
            self.header = header
        else:
            self.header = "%s %s;" % (self.header,header)
    
    def setuserdata(self, data):
        try:
            self.userData.update(data)
        except Exception:
            pass
    
    def getlogbin(self, points=100):
        logspace = np.logspace(np.log10(self.q[0]),np.log10(self.q[-1]),points)
        logelem = []
        q = np.array(self.q)
        for l in logspace:
            logelem.append((np.abs(q-l)).argmin())
        
        return list(set(logelem))
        
    def logbinq(self, points=100):
        return (np.array(self.q)[self.getlogbin(points)]).tolist()

    def logbinintensities(self, points=100):
        return (np.array(self.intensities)[self.getlogbin(points)]).tolist()
    
    def logbinerror(self, points=100):
        return (np.array(self.error)[self.getlogbin(points)]).tolist()
        
    @property
    def rootname(self):
        try:
            return "".join((self.basename.rsplit('_', 1)[0], '.dat'))
        except:
            return ""
    @property
    def rootname_rmext(self):
        return os.path.splitext(self.rootname)[0]

    @property
    def basename_rmext(self):
        return os.path.splitext(os.path.basename(self.filename))[0]
    
    @property
    def basename(self):
        return os.path.basename(self.filename)
    
    @property
    def fileindex(self):
        return self.basename_rmext.rsplit('_',1)[1]
    
    @property
    def dirname(self):
        return os.path.dirname(self.filename)
    
    @property
    def highq(self):
        return sum(self.intensities[-30:-10])
    
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
