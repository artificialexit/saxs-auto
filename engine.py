import os

import argparse

import logbook
import untangle
import redis
import pickle
import yaml

from dat import DatFile
import dat

from Pipeline import Pipeline

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
        item._attributes['ImageLocation'] = item.cdata
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
        directory,filename = os.path.split(item['ImageLocation'])
        filename = ''.join((os.path.splitext(filename)[0], '.dat'))
        
        if exp_directory == 'beamline' :
            ## For version running on beamline
            patharray = ['/data/pilatus1M'] + directory.split('/')[4:-1] + ['raw_dat',filename]
        else:
            ## Offline mode
            patharray = [exp_directory,'raw_dat',filename]

        try:
            target.send(DatFile(os.path.join(*patharray)))
        except EnvironmentError:
            pass
            

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
        directory = dat.dirname
        if exp_directory == 'beamline' :
            ## For version running on beamline
            filename = os.path.join(os.path.split(directory)[0], folder, dat.basename)
        else:
            ## Offline mode
            filename = os.path.join(exp_directory,folder,dat.basename)
        
        print "saving profile to %s" % (filename,)
        dat.save(filename)
        
@coroutine
def send_pipeline():
    while True:
        dat = (yield)
        filename = dat.filename
        epn, experiment = filename.split('/')[4:6]
        print "epn %s, experiment %s, filename %s" % (epn, experiment, dat.basename)
        pipeline.runPipeline(epn,experiment,dat.basename)
        
@coroutine
def redis_dat(channel):
    while True:
        dat =(yield)
        profile = zip(dat.q, dat.intensities)
        pickled = pickle.dumps({'filename': dat.filename, 'profile': profile})
        r.publish("logline:pub:%s" % (channel, ), pickled)
        r.set("logline:%s" % (channel, ), pickled)

@coroutine
def filter_new_sample(target):
    
    i = 0
    while True:
        
        dat = (yield)
        if i == 0:
            sub = dat
        print 'dat: %s, sub: %s' % (dat.basename,sub.basename)
        
        if dat.basename <> sub.basename:
            target.send(sub)
            sub = dat
        
        i += 1
        

@coroutine
def store_obj(obj):
    while True:
        obj.value = (yield)

@coroutine
def retrive_obj(obj, target):
    while True:
        item = (yield)
        target.send((obj.value, item))

@coroutine        
def no_op(*args, **kwargs):
 while True:
   yield
        
class Buffer(object):
    pass

if __name__ == '__main__':    
    
    parser = argparse.ArgumentParser()

    parser.add_argument('exp_directory', nargs='?', default=os.getcwd(), type=str, help="location of experiment to run auto-processor on")
    parser.add_argument('log_path', nargs='?', default='images/livelogfile.log', type=str, help="logfile path and name. Fully qualified or relative to experiment directory")
    parser.add_argument("-o","--offline", action="store_true", help="set this switch when not running on active experiment on beamline.")
    parser.add_argument("-c","--config", default='./settings.conf', action="store_true", help="use this to set config file location for pipeline")
            
    args = parser.parse_args()
        
    offline = args.offline
    exp_directory = args.exp_directory
    log_path = args.log_path
    
    try:
        stream = file(args.config, 'r') 
    except IOError:
        print "Unable to find configuration file settings.conf, exiting."
        sys.exit(2)
    
    config = yaml.load(stream)
    
    pipeline = Pipeline.Pipeline(config)
    
    if offline == True :
        redis_dat = no_op
    
    ## buffer pipeline
    buffers = filter_on_attr('SampleType', '0', load_dat(average(broadcast(save_dat('avg'), redis_dat('avg_buf'), store_obj(Buffer)))))
        
    ## samples pipeline
    massive_pipe = filter_new_sample(send_pipeline())
    subtract_pipe = retrive_obj(Buffer, subtract(broadcast(save_dat('sub'), redis_dat('avg_sub'), massive_pipe)))
    average_subtract_pipe = average(broadcast(save_dat('avg'), redis_dat('avg_smp'), subtract_pipe))
    raw_subtract_pipe = retrive_obj(Buffer, subtract(save_dat('raw_sub')))
    
    samples_pipe = broadcast(average_subtract_pipe, raw_subtract_pipe)
    samples = filter_on_attr('SampleType', '1', load_dat(samples_pipe))
    
    ## broadcast to buffers and samples
    pipe = broadcast(buffers, samples)
    
    if offline == False:
        ## Redis Beamline Version
        exp_directory = 'beamline'
        r = redis.StrictRedis(host='localhost', port=6379, db=0)
        while True:
            logline = r.hgetall(r.brpoplpush('logline:queue', 'logline:processed'))
            pipe.send(logline)
    else:
        ## File version
        # if from xml we untangle
        pipe = untangle_xml(pipe)
        with open(log_path) as logfile:
            for line in logfile:
                pipe.send(line.strip())
