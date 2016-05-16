import os
import yaml
from flask import Flask
from flask_restful import Resource, Api, reqparse, abort, fields, marshal_with
from flask_cors import CORS
from engine import Engine, Buffer
from Pipeline import PipelineLite

app = Flask(__name__)
api = Api(app)
cors = CORS(app)

parser = reqparse.RequestParser()
parser.add_argument('log_path', type=str)
parser.add_argument('exp_directory', type=str)

e = Engine()


class RestEngine(Resource):
    def put(self):

        log_path = parser.parse_args()['log_path']
        e.exp_directory = parser.parse_args()['exp_directory']

        #  Ensure directories exist:
        for subdir in ['analysis', 'avg', 'sub', 'raw_sub']:
            path = os.path.join(e.exp_directory, subdir)
            try:
                os.makedirs(path)
            except OSError:
                if not os.path.isdir(path):
                    raise

        stream = file('rest_settings.conf')
        config = yaml.load(stream)

        no_pipe = True
        e.movingAvWindow = movingAvWindow = 5
        lastname = ''

        redis_dat = e.no_op

        ## buffer pipeline
        buffers = e.filter_on_attr('SampleType', ['0', '3'], e.load_dat(e.average(e.broadcast(e.save_dat('avg'), redis_dat('avg_buf'), e.store_obj(Buffer)))))
        repbuffers = e.filter_on_attr('SampleType', ['2', '5'], e.load_dat(e.average(e.broadcast(e.save_dat('avg'), redis_dat('avg_rep_buf')))))

        ## samples pipeline
        if no_pipe is True:
            pipeline_pipe = e.filter_new_sample(e.send_pipelinelite())
        else:
            pipeline_pipe = e.no_send()

        subtract_pipe = e.retrieve_obj(Buffer, e.subtract(e.broadcast(e.save_dat('sub'), redis_dat('avg_sub'), pipeline_pipe)))
        average_subtract_pipe = e.average(e.broadcast(e.save_dat('avg'), redis_dat('avg_smp'), subtract_pipe))
        raw_subtract_pipe = e.retrieve_obj(Buffer, e.subtract(e.save_dat('raw_sub')))

        samples_pipe = e.broadcast(average_subtract_pipe)#, raw_subtract_pipe)
        samples = e.filter_on_attr('SampleType', ['1', '4'], e.load_dat(samples_pipe))

        sec_subtract_pipe = e.retrieve_obj(Buffer, e.subtract(e.broadcast(e.save_dat('sub'),e.sec_autorg())))

        sec_buffer_pipe = e.filter_on_attr_value('ImageCounter', [4, 20], e.load_dat(e.average(e.broadcast(e.save_dat('avg'), redis_dat('avg_buf'), e.store_obj(Buffer)))))
        sec_pipe = e.filter_on_attr_value('ImageCounter', [21, 5000], e.load_dat(e.moving_average(movingAvWindow, sec_subtract_pipe)))
        sec = e.filter_on_attr('SampleType', ['6'], e.broadcast(sec_pipe, sec_buffer_pipe))

        ## broadcast to buffers and samples
        pipe = e.broadcast(buffers, samples, sec)

        ## File version
        # if from xml we untangle
        untangle_pipe = e.untangle_xml(pipe)
        num_lines = sum(1 for line in open(log_path))
        with open(log_path) as logfile:
            for i,line in enumerate(logfile):
                untangle_pipe.send([line.strip(), {'flush': i+1 == num_lines}])



api.add_resource(RestEngine, '/engine')

if __name__ == '__main__':
    app.run(port=8082)
