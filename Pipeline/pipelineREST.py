# Flask based REST access to Pipeline
import os
from flask import Flask
import Pipeline
import yaml
import argparse

pipeline_app=Flask(__name__)

@pipeline_app.route("/<epn>/<exp>/<indir>/<dat>")
def loadpipelinewithdir(epn,exp,indir,dat):
    pipeline.runPipeline(epn,exp,dat,INPUTDIR=indir)
    return 'File %s from experiment %s and user %s sent to pipeline' % (epn,exp,dat)

@pipeline_app.route("/<epn>/<exp>/<dat>")
def loadpipeline(epn,exp,dat):
    pipeline.runPipeline(epn,exp,dat)
    return 'File %s from experiment %s and user %s sent to pipeline' % (epn,exp,dat)
    
if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
    parser.add_argument('exp_directory', nargs='?', default=os.getcwd(), type=str, help="location of experiment to run auto-processor on")
    parser.add_argument('log_path', nargs='?', default='images/livelogfile.log', type=str, help="logfile path and name. Fully qualified or relative to experiment directory")
    
    args = parser.parse_args()
    
    try:
        stream = file(args.config, 'r') 
    except IOError:
        print "Unable to find configuration file settings.conf, exiting."
        sys.exit(2)
    
    config = yaml.load(stream)
    
    pipeline = Pipeline.Pipeline(config)

    print 'Listening on port 8082'
    pipeline_app.run(port=8082,debug=True)