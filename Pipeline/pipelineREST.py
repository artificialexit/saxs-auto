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
    if pipeline == 'lite':
        pipelineObj =  PipelineLite.PipelineLite(os.path.join(config['RootDirectory'],epn,exp,indir,dat), os.path.join(config['RootDirectory'],epn,exp,config['PROD_PIPELINE_OUTPUT_DIR']))
        pipelineObj.runPipeline()
    else:
        pipeline.runPipeline(epn,exp,dat,INPUTDIR=indir)
    
    return 'File %s from experiment %s and user %s sent to pipeline' % (dat,exp,epn)

@pipeline_app.route("/runpipeline/<epn>/<exp>/<dat>")
#@beamline_or_vbl
def loadpipeline(epn,exp,dat):
    if pipeline == 'lite':
        pipelineObj =  PipelineLite.PipelineLite(os.path.join(config['RootDirectory'],epn,exp,'manual',dat), os.path.join(config['RootDirectory'],epn,exp,config['PROD_PIPELINE_OUTPUT_DIR']))
        pipelineObj.runPipeline()
        pass
    else: 
        pipeline.runPipeline(epn,exp,dat)
    
    return 'File %s from experiment %s and user %s sent to pipeline' % (dat,exp,epn)
    
if __name__ =='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c","--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
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
