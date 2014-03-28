# Flask based REST access to Pipeline
import os
import sys
from flask import Flask
import Pipeline, PipelineLite
import yaml
import argparse
try:
    from plugins import vbl, beamline, beamline_or_vbl
except:
    pass

pipeline_app=Flask(__name__)

@pipeline_app.route("/runpipeline/")
#@beamline_or_vbl
def landing():
    print 'here'
    return 'Landing page for runpipeline'

@pipeline_app.route("/runpipeline/<epn>/<exp>/<indir>/<dat>")
#@beamline_or_vbl
def loadpipelinewithdir(epn,exp,indir,dat):
    print epn,exp,indir,dat
    print pipeline
    if pipeline == 'lite':
        print os.path.join(config['RootDirectory'],epn,exp,indir,dat), os.path.join(config['RootDirectory'],epn,exp,config['PROD_PIPELINE_OUTPUT_DIR'])
        pipelineObj =  PipelineLite.PipelineLite(os.path.join(config['RootDirectory'],epn,exp,indir,dat), os.path.join(config['RootDirectory'],epn,exp,config['PROD_PIPELINE_OUTPUT_DIR']))
        pipelineObj.runPipeline()
    else:
        pipeline.runPipeline(epn,exp,dat,INPUTDIR=indir)
    
    return 'File %s from experiment %s and user %s sent to pipeline' % (epn,exp,dat)

@pipeline_app.route("/runpipeline/<epn>/<exp>/<dat>")
#@beamline_or_vbl
def loadpipeline(epn,exp,dat):
    if pipeline == 'lite':
        #PipelineLite.PipelineLite(os.path.join(indir,dat),os.path.dirname(dat))
        pass
    else: 
        pipeline.runPipeline(epn,exp,dat)
    
    return 'File %s from experiment %s and user %s sent to pipeline' % (epn,exp,dat)
    
if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
    parser.add_argument('exp_directory', nargs='?', default=os.getcwd(), type=str, help="location of experiment to run auto-processor on")
    parser.add_argument('log_path', nargs='?', default='images/livelogfile.log', type=str, help="logfile path and name. Fully qualified or relative to experiment directory")
    parser.add_argument("-l","--lite", action="store_true", help="set this switch to use the local lite pipeline")
    
    args = parser.parse_args()
    
    try:
        stream = file(args.config, 'r') 
    except IOError:
        print "Unable to find configuration file settings.conf, exiting."
        sys.exit(2)
    
    config = yaml.load(stream)
    
    if args.lite == False:
        pipeline = Pipeline.Pipeline(config)
    else:
        pipeline = 'lite'
    

    print 'Listening on port 8082'
    pipeline_app.run(host='0.0.0.0',port=8082)
