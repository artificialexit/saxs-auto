import os

import logbook
import untangle

from dat import DatFile
import dat

def coroutine(func):
    def start(*args,**kwargs):
        cr = func(*args,**kwargs)
        cr.next()
        return cr
    return start

@coroutine
def broadcast(*targets):
    while True:
        item = (yield)
        for target in targets:
            target.send(item)

@coroutine
def untangle_xml(target):
    while True:
        item = (yield)        
        ## do conversion (only ever has 1 child)
        item = untangle.parse(item).children[0]        
        target.send(item)
            
@coroutine
def filter_on_attr(attr, value, target):
    while True:
        item = (yield)
        if item[attr] == value:
            target.send(item)
            
@coroutine
def load_dat(target):
    while True:
        item = (yield)
        filename = ''.join((os.path.splitext(os.path.basename(item.cdata))[0], '.dat'))
        target.send(DatFile(os.path.join('data', 'dat', filename)))

@coroutine        
def average(target=None):
    dats = []
    
    while True:
        dats.append((yield))
        try:
            # root name change detection
            if dats[-2].rootname != dats[-1].rootname:
                dats = dats[-1:]
        except IndexError:
            pass
        
        if target:
            target.send(dat.average(dat.rejection(dats)))
            
        
@coroutine
def subtract(target=None):
    while True:
        buffer_, item = (yield)
        if target:
            target.send(dat.subtract(item, buffer_))
        
@coroutine
def save_dat(folder, prefix=None):
    while True:
        dat = (yield)
        dat.save(os.path.join(folder, dat.filename))

@coroutine
def store_obj(obj):
    while True:
        obj.value = (yield)
        
@coroutine
def retrive_obj(obj, target):
    while True:
        item = (yield)
        target.send((obj.value, item))
        
class Buffer(object):
    pass


if __name__ == '__main__':    
    ## buffer pipeline
    buffers = filter_on_attr('SampleType', '0', load_dat(average(broadcast(save_dat('avg'), store_obj(Buffer)))))
        
    ## samples pipeline
    subtract_pipe = retrive_obj(Buffer, subtract(save_dat('sub')))
    average_subtract_pipe = average(broadcast(save_dat('avg'), subtract_pipe))
    raw_subtract_pipe = retrive_obj(Buffer, subtract(save_dat('raw_sub')))
    
    samples_pipe = broadcast(average_subtract_pipe, raw_subtract_pipe)
    samples = filter_on_attr('SampleType', '1', load_dat(samples_pipe))
    
    ## broadcast to buffers and samples
    pipe = broadcast(buffers, samples)
    
    ## from xml not redis so we untangle
    pipe = untangle_xml(pipe)
    
    
    ## pump data through pipes
    with open("data/livelogfile.log") as logfile:
        for line in logfile:
            pipe.send(line.strip())