import os
import argparse
from dat import DatFile
import dat
from Pipeline import PipelineLite
from time import sleep

class OffsetSubtract():
    def __init__(self):
        pass
    
    def subtract(self, readpath, sample, blank, fraction=0, writepath= '../manual', analysispath='../analysis', analyse=True):

        if fraction > 0:
            fraction = round(fraction, 1)

        try:
            sampleDat = DatFile(os.path.join(readpath, sample))
            blankDat = DatFile(os.path.join(readpath, blank))
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
                try:
                    os.mkdir(writepath)
                except OSError:    
                    if not os.path.isdir(writepath):
                        print 'dat write path ' + writepath + ' invalid, file not written'
                        raise

        if analyse:
            if not os.path.exists(analysispath):
                analysispath = os.path.abspath(os.path.join(readpath,analysispath))
                if not os.path.exists(analysispath):
                    try:
                        os.mkdir(analysispath)
                    except OSError:
                        if not os.path.isdir(analysispath):
                            print 'analysis write path invalid, file not written'
                            raise

        
        num = 0
        while True:
            
            try:
                sampleTemp = DatFile(datfile=os.path.join(readpath, ''.join([sampleRoot, '_', str(sampleNum).zfill(sampleNumLen), '.dat'])))
                blankTemp = DatFile(datfile=os.path.join(readpath, ''.join([blankRoot, '_', str(blankNum).zfill(blankNumLen), '.dat'])))
                fraction_string = ''
                if fraction > 0:
                    blank2 = '{}_{:04d}.dat'.format(blankTemp.rootname_rmext, int(blankTemp.fileindex)+1)
                    blankTemp2 = DatFile(os.path.join(readpath, blank2))
                    blankTemp = dat.average([blankTemp, blankTemp2], [fraction, 1-fraction])
                    fraction_string = '_{:.0f}'.format(100*fraction)

                subtractedDat = dat.subtract(sampleTemp, blankTemp)

                filename = os.path.join(writepath, ''.join([os.path.splitext(subtractedDat.rootname)[0], '_', str(sampleNum).zfill(sampleNumLen), '_', str(blankNum).zfill(blankNumLen), fraction_string, '.dat']))
                subtractedDat.save(filename)

                if analyse:
                    pipeline = PipelineLite.PipelineLite(filename, analysispath)
                    pipeline.runPipeline()

                sampleNum += 1
                blankNum += 1
                num += 1

            except IOError as e:
                print 'IO'
                break
            
            except OSError as e:
                print 'OS'
                break
            
            except Exception:
                raise
        
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
   
    