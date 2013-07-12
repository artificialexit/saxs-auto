import os

import argparse
import sys
import logbook
import untangle
import redis
import pickle
import yaml

from dat import DatFile
import dat

from Pipeline import Pipeline
from Pipeline import PipelineLite

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
        if item[attr] in value:
            target.send(item)

@coroutine
def filter_on_attr_value(attr, value, target):
    while True:
        item = (yield)
        if float(item[attr]) >= value[0] and float(item[attr]) <= value[1]:
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
def moving_average(number,target=None):
    dats = []
    
    while True:
        dats.append((yield))
        try:
            # root name change detection
            if dats[-2].rootname != dats[-1].rootname:
                dats = dats[-1:]
        except IndexError:
            pass
        
        if len(dats) > number:
            dats = dats[-number:]
        if target:
            #goodDats = dat.rejection(dats)
            #if goodDats != None:
            #    result = dat.average(goodDats)
            #    result.filename = dats[0].filename
            #    target.send(result)

            result = dat.average(dats)
            result.filename = dats[0].filename
            target.send(result)

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
            goodDats = dat.rejection(dats)
            if goodDats != None:
                target.send(dat.average(goodDats))
            
        
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
        print "send to pipeline"
        pipeline.runPipeline(epn,experiment,dat.basename)

@coroutine
def send_pipelinelite():
    while True:
        dat = (yield)
        filename = dat.filename
        analysis_path = os.path.dirname(os.path.dirname(filename))+'/analysis/'
        litePipeline = PipelineLite.PipelineLite(filename,analysis_path)
        print "run pipelinelite"
        litePipeline.runPipeline()

@coroutine
def sec_autorg():
    
    secrunname =''
    
    while True:
        dat = (yield)
       
        datfilename = dat.filename
        analysis_path = os.path.dirname(os.path.dirname(datfilename))+'/analysis/'
        litePipeline = PipelineLite.PipelineLite(datfilename,analysis_path)
        autorg = litePipeline.autorg()
        
        if secrunname <> dat.rootname:
            secrunname = dat.rootname
           
            filename = datfilename.split('/')[-1] 
            if filename.endswith('.dat'):
                filename = filename[:-4] 
           
            with open(analysis_path+'/'+filename+'_rgtrace.dat','w') as outputfile :
                outputfile.write('index Rg I0 quality\n')
           
            rgarray =[]
            indexArray =[]
            I0Array=[]
            qualityArray=[]
            count = 0
        
        if count < movingAvWindow-1:
            count = count + 1
            continue
        
        with open(analysis_path+'/'+filename+'_rgtrace.dat','a') as outputfile :
            numFields = len(autorg.split(" "))
            rg = (autorg.split(" "))[0]
            I0 = (autorg.split(" "))[2]
            
            if rg == 'Error:':
                rg = '0'
                I0 = '0'

            if numFields >= 8:
                quality = (autorg.split(" "))[6]
            else:
                quality = '0'

            index = dat.fileindex
            outputfile.write('%s %s %s %s\n' % (index,rg,I0,quality))
            indexArray.append(int(index))
            rgarray.append(float(rg))
            I0Array.append(float(I0))
            try:
                qualityArray.append(float(quality))
            except ValueError:
                qualityArray.append(0.0)
                
            
        rgprofile = zip(indexArray,rgarray,I0Array,qualityArray)
        pickled = pickle.dumps({'profiles': rgprofile})
        r.set("pipeline:sec:Rg", pickled)
        r.publish("pipeline:sec:pub:Rg", "NewRg")
        
@coroutine
def redis_dat(channel):
    while True:
        dat =(yield)
        profile = zip(dat.q, dat.intensities, dat.errors)
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
def retrieve_obj(obj, target):
    while True:
        item = (yield)
        try:                
            target.send((obj.value, item))
        except AttributeError:
            pass

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
    parser.add_argument("-c","--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
    parser.add_argument("-l","--lite", action="store_true", help="set this switch to use the local lite pipeline")
            
    args = parser.parse_args()
    offline = args.offline
    exp_directory = args.exp_directory
    log_path = args.log_path
    print args.config
    
    try:
        stream = file(args.config, 'r') 
    except IOError:
        print "Unable to find configuration file settings.conf, exiting."
        sys.exit(2)
    
    config = yaml.load(stream)
    
    movingAvWindow = 5
    
    pipeline = Pipeline.Pipeline(config)
    
    if offline == False :
        redis_dat = no_op
    else:
        ##Load last buffer from redis incase this is a recovery
        r = redis.StrictRedis(host='localhost', port=6379, db=0)
        redisBuffer = r.get('logline:avg_buf')
        try :            
            bufferQ,bufferProfile,bufferErrors = zip(*pickle.loads(redisBuffer))
            bufferDat = DatFile()
            bufferDat.q = bufferQ
            bufferDat.intensities = bufferProfile
            bufferDat.errors = bufferErrors
            Buffer = Buffer()
            Buffer.value = bufferDat
        except:
            pass
    
    ## buffer pipeline
    buffers = filter_on_attr('SampleType', ['0','3'], load_dat(average(broadcast(save_dat('avg'), redis_dat('avg_buf'), store_obj(Buffer)))))
        
    ## samples pipeline
    
    if args.lite == False:
        pipeline_pipe = filter_new_sample(send_pipeline())
    else:
        pipeline_pipe = filter_new_sample(send_pipelinelite())
    
    subtract_pipe = retrieve_obj(Buffer, subtract(broadcast(save_dat('sub'), redis_dat('avg_sub'), pipeline_pipe)))
    average_subtract_pipe = average(broadcast(save_dat('avg'), redis_dat('avg_smp'), subtract_pipe))
    raw_subtract_pipe = retrieve_obj(Buffer, subtract(save_dat('raw_sub')))
    
    samples_pipe = broadcast(average_subtract_pipe, raw_subtract_pipe)
    samples = filter_on_attr('SampleType', ['1','4'], load_dat(samples_pipe))
    
    sec_subtract_pipe = retrieve_obj(Buffer, subtract(broadcast(save_dat('sub'),sec_autorg())))
    
    sec_buffer_pipe = filter_on_attr_value('ImageCounter',[4,20],load_dat(average(broadcast(save_dat('avg'),redis_dat('avg_buf'),store_obj(Buffer)))))
    sec_pipe = filter_on_attr_value('ImageCounter',[21,5000], load_dat(moving_average(movingAvWindow,sec_subtract_pipe)))
    sec = filter_on_attr('SampleType',['6'], broadcast(sec_pipe, sec_buffer_pipe))
    
    ## broadcast to buffers and samples
    pipe = broadcast(buffers, samples, sec)
    
    if offline == False:
        ## Redis Beamline Version
        exp_directory = 'beamline'
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
