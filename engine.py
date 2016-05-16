import os
import time
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
        def start(*args, **kwargs):
            cr = func(*args, **kwargs)
            cr.next()
            return cr
        return start


class Engine(object):

    def __init__(self):
        self.exp_directory = None
        self.lastname = None
        self.movingAvWindow = None

    @coroutine
    def broadcast(self, *targets):
        while True:
            item = (yield)
            for target in targets:
                target.send(item)

    @coroutine
    def untangle_xml(self, target):
        while True:
            item = (yield)
            data = item[1]
            item = item[0]
            ## do conversion (only ever has 1 child)
            item = untangle.parse(item).children[0]
            item._attributes['ImageLocation'] = item.cdata
            for attr in data:
                item._attributes[attr] = data[attr]
            target.send(item)

    @coroutine
    def filter_on_attr(self, attr, value, target):
        while True:
            item = (yield)
            if item[attr] in value:
                target.send(item)

    @coroutine
    def filter_on_attr_value(self, attr, value, target):
        while True:
            item = (yield)
            if value[0] <= float(item[attr]) <= value[1]:
                target.send(item)

    @coroutine
    def load_dat(self, target):

        while True:

            item = (yield)
            directory,filename = os.path.split(item['ImageLocation'])
            filename = ''.join((os.path.splitext(filename)[0], '.dat'))

            if self.exp_directory == 'beamline' :
                ## For version running on beamline
                patharray = ['/data/pilatus1M'] + directory.split('/')[2:-1] + ['raw_dat', filename]
                print patharray
            else:
                ## Offline mode
                patharray = [self.exp_directory, 'raw_dat', filename]

            try:

                filesize = 0
                for count in range(30):
                    filesizetemp = os.path.getsize(os.path.join(*patharray))
                    if filesizetemp == filesize:
                        break
                    filesize=filesizetemp
                    time.sleep(0.1)

                dat = DatFile(os.path.join(*patharray))
                try:
                    dat.setuserdata({'flush': item['flush']})
                except Exception:
                    pass

                target.send(dat)
            except EnvironmentError:
                pass


    @coroutine
    def moving_average(self, number, target=None):
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
                result.setuserdata({'rawhighq': result.highq})
                target.send(result)

    @coroutine
    def average(self, target=None):

        dats = []

        while True:
            dats.append((yield))
            try:
                # root name change detection
                if self.lastname != dats[-1].rootname:
                    dats = dats[-1:]
            except IndexError:
                pass

            self.lastname = dats[-1].rootname

            if target:
                goodDats = dat.rejection(dats)
                if goodDats is not None:
                    target.send(dat.average(goodDats))


    @coroutine
    def subtract(self, target=None):
        while True:
            buffer_, item = (yield)
            if target:
                target.send(dat.subtract(item, buffer_))

    @coroutine
    def save_dat(self, folder, prefix=None):
        while True:
            dat = (yield)
            directory = dat.dirname
            if self.exp_directory == 'beamline' :
                ## For version running on beamline
                filename = os.path.join(os.path.split(directory)[0], folder, dat.basename)
            else:
                ## Offline mode
                print 'save'
                filename = os.path.join(self.exp_directory, folder, dat.basename)

            print "saving profile to %s" % (filename,)
            dat.save(filename)

    @coroutine
    def send_pipeline(self):
        while True:
            dat = (yield)
            filename = dat.filename
            print filename
            dir1, epn, experiment = filename.split('/')[-5:-2]
            if epn in config['DetectorFolders']:
                experiment = os.path.join(epn,experiment)
                epn = dir1
            print "epn %s, experiment %s, filename %s" % (epn, experiment, dat.basename)
            print "send to pipeline"
            pipeline.runPipeline(epn, experiment, dat.basename)

    @coroutine
    def send_pipelinelite(self):
        while True:
            dat = (yield)
            filename = dat.filename
            analysis_path = os.path.dirname(os.path.dirname(filename))+'/analysis/'
            litePipeline = PipelineLite.PipelineLite(filename, analysis_path)
            print "run pipelinelite"
            litePipeline.runPipeline()

    @coroutine
    def no_send(self):
        while True:
            dat = (yield)
            print "Finished %s" % (dat,)

    @coroutine
    def sec_autorg(self):

        secrunname =''

        while True:
            dat = (yield)

            datfilename = dat.filename
            analysis_path = os.path.dirname(os.path.dirname(datfilename))+'/analysis/'
            litePipeline = PipelineLite.PipelineLite(datfilename, analysis_path)
            autorg = litePipeline.autorg()

            if secrunname != dat.rootname:
                secrunname = dat.rootname

                filename = datfilename.split('/')[-1]
                if filename.endswith('.dat'):
                    filename = filename[:-4]

                with open(analysis_path+'/'+filename+'_rgtrace.dat','w') as outputfile:
                    outputfile.write('index Rg I0 quality IntAtHighQ\n')

                rgarray =[]
                indexArray =[]
                I0Array=[]
                qualityArray=[]
                highqArray=[]
                count = 0

            if count < self.movingAvWindow-1:
                count = count + 1
                continue

            with open(analysis_path+'/'+filename+'_rgtrace.dat','a') as outputfile:
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

                index = str(int(dat.fileindex) + self.movingAvWindow/2)
                outputfile.write('%s %s %s %s %s\n' % (index, rg, I0, quality, dat.userData['rawhighq']))
                indexArray.append(int(index))
                rgarray.append(float(rg))
                I0Array.append(float(I0))
                try:
                    qualityArray.append(float(quality))
                except ValueError:
                    qualityArray.append(0.0)
                highqArray.append(dat.userData['rawhighq'])

            rgprofile = zip(indexArray, rgarray, I0Array, qualityArray, highqArray)
            try:
                pickled = pickle.dumps({'profiles': rgprofile})
                r.sadd("pipeline:sec:filenames", '%s/%s' % (dat.dirname, dat.rootname))
                r.set("pipeline:sec:%s/%s:Rg" % (dat.dirname, dat.rootname), pickled)
                r.publish("pipeline:sec:pub:Filename", '%s/%s' % (dat.dirname, dat.rootname))
            except NameError:  # No redis defined - running with offline switch
                pass

    @coroutine
    def redis_dat(self, channel):
        while True:
            dat =(yield)
            profile = zip(dat.q, dat.intensities, dat.errors)
            pickled = pickle.dumps({'filename': dat.filename, 'profile': profile})
            r.publish("logline:pub:%s" % (channel, ), pickled)
            r.set("logline:%s" % (channel, ), pickled)

    @coroutine
    def filter_new_sample(self, target):

        i = 0
        while True:

            dat = (yield)
            if i == 0:
                sub = dat
            print 'dat: %s, sub: %s' % (dat.basename, sub.basename)

            if dat.basename != sub.basename:
                target.send(sub)
                sub = dat

            if 'flush' in dat.userData:
                if dat.userData['flush'] is True:
                    target.send(dat)

            i += 1


    @coroutine
    def store_obj(self, obj):
        while True:
            obj.value = (yield)

    @coroutine
    def retrieve_obj(self, obj, target):
        while True:
            item = (yield)
            try:
                target.send((obj.value, item))
            except AttributeError:
                print "attr error"
                pass
            except Exception as e:
                print "other error"
                print e.__repr__()
                pass

    @coroutine
    def no_op(self, *args, **kwargs):
        while True:
            yield


class Buffer(object):
    pass

if __name__ == '__main__':    

    engine = Engine()

    parser = argparse.ArgumentParser()
    parser.add_argument('exp_directory', nargs='?', default=os.getcwd(), type=str, help="location of experiment to run auto-processor on")
    parser.add_argument('log_path', nargs='?', default='images/livelogfile.log', type=str, help="logfile path and name. Fully qualified or relative to experiment directory")
    parser.add_argument("-o", "--offline", action="store_true", help="set this switch when not running on active experiment on beamline.")
    parser.add_argument("-c", "--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
    parser.add_argument("-l", "--lite", action="store_true", help="set this switch to use the local lite pipeline")
    parser.add_argument("-n", "--none", action="store_true", help="set this switch to not use the pipeline")
            
    args = parser.parse_args()
    offline = args.offline
    engine.exp_directory = exp_directory = args.exp_directory
    log_path = args.log_path

    if args.log_path[0] != '/':
        log_path = os.path.normpath(args.exp_directory) + '/' + log_path

    print args.config
    
    try:
        stream = file(args.config, 'r')
    except IOError:
        print "Unable to find configuration file %s. Trying ./settings.conf." % (args.config)
        try:
            stream = file('settings.conf')
        except IOError:
            print "Unable to find configuration file settings.conf. Exiting..."
            sys.exit(2)
    
    config = yaml.load(stream)
    
    engine.movingAvWindow = movingAvWindow = 5
    lastname = ''

    pipeline = Pipeline.Pipeline(config)
    
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    
    if offline is True:
        redis_dat = no_op

    else:
        ##Load last buffer from redis in case this is a recovery
        redisBuffer = r.get('logline:avg_buf')
        try :            
            bufferQ, bufferProfile, bufferErrors = zip(*pickle.loads(redisBuffer))
            bufferDat = DatFile()
            bufferDat.q = bufferQ
            bufferDat.intensities = bufferProfile
            bufferDat.errors = bufferErrors
            Buffer = Buffer()
            Buffer.value = bufferDat
        except:
            pass
    
    ## buffer pipeline
    buffers = engine.filter_on_attr('SampleType', ['0', '3'], engine.load_dat(engine.average(engine.broadcast(engine.save_dat('avg'), redis_dat('avg_buf'), engine.store_obj(Buffer)))))
    repbuffers = engine.filter_on_attr('SampleType', ['2', '5'], engine.load_dat(engine.average(engine.broadcast(engine.save_dat('avg'), redis_dat('avg_rep_buf')))))
        
    ## samples pipeline
    if args.none is False:
        if args.lite is False:
            print 'pipeline'
            pipeline_pipe = engine.filter_new_sample(engine.send_pipeline())
        else:
            pipeline_pipe = engine.filter_new_sample(engine.send_pipelinelite())
    else:
        pipeline_pipe = engine.no_send()

    subtract_pipe = engine.retrieve_obj(Buffer, engine.subtract(engine.broadcast(engine.save_dat('sub'), redis_dat('avg_sub'), pipeline_pipe)))
    average_subtract_pipe = engine.average(engine.broadcast(engine.save_dat('avg'), redis_dat('avg_smp'), subtract_pipe))
    raw_subtract_pipe = engine.retrieve_obj(Buffer, engine.subtract(engine.save_dat('raw_sub')))
    
    samples_pipe = engine.broadcast(average_subtract_pipe)#, raw_subtract_pipe)
    samples = engine.filter_on_attr('SampleType', ['1', '4'], engine.load_dat(samples_pipe))
    
    sec_subtract_pipe = engine.retrieve_obj(Buffer, engine.subtract(engine.broadcast(engine.save_dat('sub'),engine.sec_autorg())))
    
    sec_buffer_pipe = engine.filter_on_attr_value('ImageCounter', [4, 20], engine.load_dat(engine.average(engine.broadcast(engine.save_dat('avg'), redis_dat('avg_buf'), engine.store_obj(Buffer)))))
    sec_pipe = engine.filter_on_attr_value('ImageCounter', [21, 5000], engine.load_dat(engine.moving_average(movingAvWindow, sec_subtract_pipe)))
    sec = engine.filter_on_attr('SampleType', ['6'], engine.broadcast(sec_pipe, sec_buffer_pipe))
    
    ## broadcast to buffers and samples
    pipe = engine.broadcast(buffers, samples, sec)
    
    if offline is False:
        ## Redis Beamline Version
        exp_directory = 'beamline'
        while True:
            logline = r.hgetall(r.brpoplpush('logline:queue', 'logline:processed'))
            pipe.send(logline)
    else:
        ## File version
        # if from xml we untangle
        untangle_pipe = engine.untangle_xml(pipe)
        num_lines = sum(1 for line in open(log_path))
        with open(log_path) as logfile:
            for i,line in enumerate(logfile):
                untangle_pipe.send([line.strip(), {'flush': i+1 == num_lines}])
