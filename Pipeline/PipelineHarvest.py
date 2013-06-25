#!/usr/bin/python

import getopt
import sys
import logging
import os

try:
    import yaml
    import redis
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.


class PipelineHarvest:
    def __init__(self, config):
        self.redis = redis.StrictRedis(host='localhost', port=6379, db=1)
        self.name = "PipelineHarvest"
        self.log = None
        self.config = config
        
        self.TYPE               = None
        self.FILE_TO_HARVEST    = None
        self.ExperimentFolderOn = None
        
        #Read all configuration settings
        self.setConfiguration()
        # Set log
        self.setLog()
        
    def setConfiguration(self):
        self.ExperimentFolderOn = self.config.get("ExperimentFolderOn")
        
    def setLog(self):
        # set logging
        log_filename = '../logs/'+self.name+'.log'
        self.log = logging.getLogger(log_filename)
        hdlr = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s]: %(message)s')
        hdlr.setFormatter(formatter)
        self.log.addHandler(hdlr)
        
    def runHarvest(self, value_type, file_to_harvest):
        # Set target user, experiment and datfile
        self.TYPE            = value_type
        self.FILE_TO_HARVEST = file_to_harvest
        
        
        path = str(file_to_harvest)
        path = path.rstrip('/')
        folders = path.split('/')
        
        
        if self.ExperimentFolderOn: 
            # database name is user folder name plus and experiment folder name
            # eg. /beam/user/myexp/analysis/file_to_harvest
            user_folder = folders[-4]
            experiment_folder = folders[-3]
            database_name = user_folder + '_' + experiment_folder
        else:
            # database name would be only user folder name
            # eg. /beam/user/analysis/file_to_harvest
            user_folder = folders[-3]
            database_name = user_folder
        
        if os.path.isfile(file_to_harvest):
            f = open(file_to_harvest, 'r')
            value = str(f.read())
            value = value.replace('\n','')
            f.close()

            if value_type == "autorg":
                # save info from autorg to database
                datfile = str(folders[-1]).replace('_autorg.out', '.dat')
                values = value.split(' ')
                keys = ['AutoRG_RG', 'AutoRG_RGE', 'AutoRG_I0', 'AutoRG_I0E', 'AutoRG_Start', 'AutoRG_End', 'AutoRG_Quality', 'AutoRG_ValidFlag']
                valueMap = {keys[i]: values[i] for i in range(8)}
                self.redis.hmset('pipeline:results:' + datfile, valueMap)

            elif value_type == "porod_volume":
                # save "porod volume" value to database
                datfile = str(folders[-1]).replace('_porod_volume.out', '.dat')
                self.redis.hset('pipeline:results:' + datfile, 'PorodVolume', value)
                
            elif value_type == "dam_volume":
                # save "Total excluded DAM volume" value to database
                datfile = str(folders[-1]).replace('_dam_volume', '')[:-2] + '.dat'
                number = str(folders[-1]).replace('_dam_volume', '')[-1:]                
                pdbfile = str(folders[-1]).replace('_dam_volume', '-1.pdb')
                self.redis.hset('pipeline:results:' + datfile, 'DamVolume'+number, value)
                self.redis.hset('pipeline:results:' + datfile, 'DamPDB'+number, pdbfile)
            
            self.redis.lpush('pipeline:results:queue','pipeline:results:' + datfile)
            if self.redis.zscore('pipeline:results:set', 'pipeline:results:' + datfile) == None:
                numResults = self.redis.zcard('pipeline:results:set')
                self.redis.zadd('pipeline:results:set', numResults+1, 'pipeline:results:' + datfile)
            
            #numResults = self.redis.zcard('pipeline:results:set')
            #resultKeys = self.redis.zrange('pipeline:results:set',0,numResults-1)
            #file = open('path'+'/analysis.dat','w')
            #for key in resultKeys:
            #    results = self.redis.hgetall(key)
            #    filename = key.split(':')[-1]
            #    file.write('%s,%s'

if __name__ == "__main__":
    configuration = "../settings.conf"
    value_type = ""
    file_to_harvest = ""
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:t:f:", ["config", "type", "file"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        sys.exit(2)

    # get prefix option, example: -p /data_home/user_epn/user_exp/analysis/sample        
    for o, a in opts:
        if o in ("-c", "--conf"):
            configuration = str(a)
        if o in ("-t", "--type"):
            value_type = str(a)
        if o in ("-f", "--file"):
            file_to_harvest = str(a)

    if value_type not in ["autorg", "porod_volume", "dam_volume", "damaver"]:
        print "ERROR: Invalid type '%s'. One of 'porod_volume', 'dam_volume', or 'damaver'." % (value_type)
        sys.exit(2)
    
    try:
        stream = file(configuration, 'r') 
    except IOError:
        print "Unable to find configuration file settings.conf, exiting."
        sys.exit(2)
        
    config = yaml.load(stream)
    
    pipeline_harvest = PipelineHarvest(config)
    pipeline_harvest.runHarvest(value_type, file_to_harvest)    
