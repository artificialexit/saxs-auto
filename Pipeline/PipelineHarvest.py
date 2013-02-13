#!/usr/bin/python

import getopt
import sys
import logging
import os

try:
    import yaml
    from sqlalchemy import create_engine
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import Column, Integer, String
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.

from models import AverageSubtractedImages
from models import DamVolumes

class PipelineHarvest:
    def __init__(self, config):
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
        
    def createDBEngine(self, database_name):
        database = self.config.get('database')
        engine_str = "mysql+mysqldb://%s:%s@%s/%s" % (database['user'], database['passwd'], database['host'], database_name)
        db_engine = create_engine(engine_str)
        
        return db_engine

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
        
        if value_type == "porod_volume":
            table_name = "average_subtracted_images"
        elif value_type == "dam_volume":
            table_name = "dam_volumes"
        
        engine = self.createDBEngine(database_name)
        Session = sessionmaker(bind=engine)
        session = Session()
        

        if os.path.isfile(file_to_harvest):
            f = open(file_to_harvest, 'r')
            value = str(f.read())
            value = value.replace('\n','')
            f.close()

            if value_type == "porod_volume":
                # save "porod volume" value to database
                datfile = str(folders[-1]).replace('_porod_volume', '.dat')
                obj = session.query(AverageSubtractedImages).filter_by(average_subtracted_location=datfile).first() 
                obj.porod_volume = value
                session.commit()
                
                
            elif value_type == "dam_volume":
                # save "Total excluded DAM volume" value to database
                datfile = str(folders[-1]).replace('_dam_volume', '')[:-2] + '.dat'
                pdbfile = str(folders[-1]).replace('_dam_volume', '-1.pdb')
                
                datfile_id = session.query(AverageSubtractedImages).filter_by(average_subtracted_location=datfile).first().id
                obj = DamVolumes(dammif_pdb_file=pdbfile, dam_volume=value, average_subtracted_images_fk=datfile_id)
                session.add(obj)
                session.commit()
                

            
            
            
  

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

    if value_type not in ["porod_volume", "dam_volume", "damaver"]:
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