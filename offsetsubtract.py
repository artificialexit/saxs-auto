import os
import argparse
from dat import DatFile
import dat
from Pipeline import PipelineLite
from time import sleep

class OffsetSubtract():
    def __init__(self):
        pass
    
    def subtract(self, readpath, sample, blank, writepath='../manual', analysispath='../analysis', analyse=True):
        try:
            sampleDat = DatFile(os.path.join(readpath,sample))
            blankDat = DatFile(os.path.join(readpath,blank))
        except IOError:
            print "File Doesn't Exist"
            return -1
        
        sampleNum = int(sampleDat.fileindex)
        blankNum = int(blankDat.fileindex)
        sampleNumLen = len(sampleDat.fileindex)
        blankNumLen = len(blankDat.fileindex)
        

        sampleRoot = os.path.splitext(sampleDat.rootname)[0]
        blankRoot = os.path.splitext(blankDat.rootname)[0]
                
        if not os.path.exists(writepath):
            writepath = os.path.abspath(os.path.join(readpath,writepath))
            if not os.path.exists(writepath):
                print 'dat write path invalid, file not written'
                return -1

        if analyse:
            if not os.path.exists(analysispath):
                analysispath = os.path.abspath(os.path.join(readpath,analysispath))
                if not os.path.exists(analysispath):
                    print 'analysis write path invalid, file not written'
                    return -1
                
        
        num = 0
        while True:
            
            try:
                sampleTemp = DatFile(datfile=os.path.join(readpath,''.join([sampleRoot,'_',str(sampleNum).zfill(sampleNumLen),'.dat'])))
                blankTemp = DatFile(datfile=os.path.join(readpath,''.join([blankRoot,'_',str(blankNum).zfill(blankNumLen),'.dat'])))

                subtractedDat = dat.subtract(sampleTemp, blankTemp)
                filename = os.path.join(writepath,''.join([os.path.splitext(subtractedDat.rootname)[0],'_',str(sampleNum).zfill(sampleNumLen),'_',str(blankNum).zfill(blankNumLen),'.dat']))
                subtractedDat.save(filename)

                sampleNum += 1
                blankNum += 1
                num += 1

                if analyse:
                    pipeline = PipelineLite.PipelineLite(filename,analysispath)
                    pipeline.runPipeline()

            except OSError as e:
                print e
                break
        
        return num
        
        

if __name__ == '__main__':    
    
    parser = argparse.ArgumentParser()
    parser.add_argument('readpath', nargs='?', default=os.getcwd(),type=str, help="Path to experimental data (dat files).")
    parser.add_argument('firstsample', nargs='?', default='', type=str, help="filename of first sample dat")
    parser.add_argument('firstblank', nargs='?', default='', type=str, help="filename of first blank dat")
    parser.add_argument("-d","--datwritepath", default="../manual", action="store", help="set dat output directory. Either fully qualified or relative to readpath")
    parser.add_argument("-a","--analysiswritepath", default="../analysis", action="store", help="set analysis output directory. Either fully qualified or relative to readpath")
    parser.add_argument("-o","--offline", action="store_true", help="set this switch when not running on active experiment on beamline.")
    parser.add_argument("-c","--config", default='/beamline/apps/saxs-auto/settings.conf', action="store", help="use this to set config file location for pipeline")
    parser.add_argument("-l","--lite", action="store_true", help="set this switch to use the local lite pipeline")
            
    args = parser.parse_args()
    
    a = OffsetSubtract()
    num = a.subtract(args.readpath,args.firstsample,args.firstblank)
    print num
   
    