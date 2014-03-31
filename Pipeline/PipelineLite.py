#!/usr/bin/python

import os
import subprocess
import sys
import time
import getopt
import PipelineHarvest

class PipelineLite:
    """
    Takes a path to a datFile as an input file, then runs the local pipeline 
    analysis workflow.
     
    Args:
        datFilePath (String): Absolute location of the datFile as told from the local machine
    """
    def __init__(self, datFilePath, outputPath):
        if not datFilePath.endswith('.dat'):
            print "ERROR: *.dat file is expected as input file."
            sys.exit(2)
        self.datFilePath = datFilePath
        if not outputPath.endswith('/'):
            outputPath += '/'
        self.outputPath = outputPath

    def runPipeline(self, harvest=True):
        """
        Runs pipeline with multiple analysis steps.
        """
        print 'Running piplinelite'
        # autorg modeling
        autorg_output = self.autorg()
        print autorg_output
        # Write out autorg
        self.saveAutoRgVolume(autorg_output)
        # datgnom modeling
        outfile_path = self.datgnom(autorg_output)
        # datporod modeling
        porod_volume = self.datporod(outfile_path)
        # store datporod volume (porod volume)
        self.saveDatporodVolume(porod_volume)
        # dammif modeling with fast mode
        # store dam volume (total excluded DAM volume)
        # dam_volume = self.dammif(outfile_path)
        # self.saveDammifVolume(dam_volume)
        if harvest:
            rawfilename = os.path.splitext(os.path.basename(self.datFilePath))[0]
            print rawfilename
            harvest = PipelineHarvest.PipelineHarvest({"ExperimentFolderOn" : True})
            harvest.runHarvest('porod_volume',self.outputPath + rawfilename + '_porod_volume.dat')
            harvest.runHarvest('autorg',self.outputPath + rawfilename + '_autorg.out')
    
    def autorg(self):
        """
        Automatically computes Rg and I(0) using the Guinier approximation, 
        estimates data quality, finds the beginning of the useful data range.
        """
        #print '#---- autorg -----------------------#'
        command_list = ['autorg', '-f', 'ssv', self.datFilePath]
        process = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, error_output) = process.communicate()
        #print ' '.join(command_list)
        #print error_output
        if output == '' :
            return "Error: " + error_output
        return output 
    
    def saveAutoRgVolume(self, autorg_output):
        """
        Writes AutoRg output to disk 
        """
        print '#---- save AutoRg Output------------#'    
        print 'autorg_output =', autorg_output, '\n'
        
        file = self.datFilePath.split('/')[-1] 
        if file.endswith('.dat'):
            #eg: filename="sample" if input file is /input_path/sample.dat
            filename = file[:-4] 
        
        outfile_path = self.outputPath + filename + '_autorg.out'
        f = open(outfile_path, 'w')
        f.write(autorg_output)
        f.close()
        
    
    def datgnom(self, autorg_output):
        """
        Estimates Dmax, computes the distance distribution function p(r) and the 
        regularized scattering curve.
        """
        print '#---- datgnom ----------------------#'    
        
        #eg: file="sample.dat" if input file is /input_path/sample.dat
        file = self.datFilePath.split('/')[-1] 
        if file.endswith('.dat'):
            #eg: filename="sample" if input file is /input_path/sample.dat
            filename = file[:-4] 
            
        outfile_path = self.outputPath + filename + '_datgnom.out'
        
        valuePoints = autorg_output.split(" ")
        
        if valuePoints[0] == "Error:":
            
            f = open(outfile_path, 'w')
            f.write("Error with autorg input.")
            f.close()
            return outfile_path
     
        rg = valuePoints[0]
        try:
            skip = valuePoints[4]
        except IndexError:
            f = open(outfile_path, 'w')
            f.write("Error with autorg input (not enough fields).")
            f.close()
            return outfile_path
        
        try:
            skip = int(skip)
            skip = skip - 1
        except ValueError:
            print "Error happened when converting skip value into integer."
                
        command_list = ['datgnom', '-r', str(rg), '-s', str(skip), '-o', outfile_path, self.datFilePath]
        process = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, error_output) = process.communicate()
        # it generates a gnom output file (*.out)
        print ' '.join(command_list)
        print output
        return outfile_path 
    
    def datporod(self, outfile_path):
        """
        Computes Porod volume from the regularised scattering curve.
        """
        print '#---- datporod ---------------------#' 
        command_list = ['datporod', outfile_path]
        process = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, error_output) = process.communicate()
        porod_volume = str(output).strip(' ').split(' ')[0]
        print ' '.join(command_list)
        print output
        print error_output
        if output == '' :
            return "Error: " + error_output
        return porod_volume
    
    def saveDatporodVolume(self, porod_volume):
        """
        Writes value of Porod volume to disk. 
        """
        print '#---- save porod volume------------#'    
        print 'porod_volume =', porod_volume, '\n'
        file = self.datFilePath.split('/')[-1] 
        if file.endswith('.dat'):
            #eg: filename="sample" if input file is /input_path/sample.dat
            filename = file[:-4] 
        
        outfile_path = self.outputPath + filename + '_porod_volume.out'
        f = open(outfile_path, 'w')
        f.write(porod_volume)
        f.close()

    
    def dammif(self, outfile_path):
        """
        Creates an ab initio dummy atoms model, estimates DAM volume.
        """
        print '#---- dammif ----------------------#'    
        prefix = outfile_path[:-4] + "_0"
        command_list = ['dammif', '--prefix=%s' % prefix, '--mode=fast', '--symmetry=P1', '--unit=n', outfile_path]
        process = subprocess.Popen(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        (output, error_output) = process.communicate()
        print ' '.join(command_list)
        # monitor if "Total excluded DAM volume" value exists in output file *-1.pdb
        start_time = time.time()
        pdbfile_path = prefix + "-1.pdb"
        fitfile_path = prefix + ".fit"
        while (1):
            # monitor if dammif modelling process has finished
            # monitor the existence of dammif output file *.fit file which is 
            # generated in the end of dammif process.
            if os.path.isfile(fitfile_path) and os.path.isfile(pdbfile_path):
                pdbfile = open(pdbfile_path, 'r')
                search = 'Total excluded DAM volume'
                for line in pdbfile:
                    if line.find(search) > -1:
                        dam_volume = line.split(':')[1].strip(' ')
                        print 'Total excluded DAM volume value found: ', dam_volume
                        # break text search
                        break
                # break waiting
                break
            else:
                # keep waiting
                time.sleep(1)
            # exceed 100 seconds then enforce to terminate dammif execution
            if time.time() - start_time > 100:
                # force to break waiting
                break
       
        return dam_volume 
    
    def saveDammifVolume(self, dam_volume):
        """
        Stroes value of DAM volume into database.
        """
        print '#---- save dam volume -------------#'    
        print 'dam_volume =', dam_volume, '\n'
        "TODO: save value of dammif volume into database."
        

def usage():
    """
    Usage: ./PipelineLite.py [OPTIONS] -f /full/path/filename.dat -o /output/full/path/
    
    -d --datfile       The full path of your SAXS experimental data file to be used for models.
    
    -o --output_path   The full directory path for all output files generated during pipeline modeling. 
    """
    print 'Usage: %s [OPTIONS] -f /full/path/filename.dat -o /output/full/path/ \n' % (sys.argv[0])
    print '''
              
-d --datfile       The full path of your SAXS experimental data file to be used 
                   for models.
               
-o --output_path   The full directory path for all output files generated during
                   pipeline modeling. 
               
'''

if __name__ == "__main__":
    datfile = ""
    output_path = "."
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "d:o:", ["datfile", "output_path"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    # get options  
    for o, a in opts:
        if o in ("-d", "--datfile"):
            datfile = str(a)
        if o in ("-o", "--output_path"):
            output_path = str(a)

    if not datfile.endswith('.dat'):
        print "ERROR: *.dat file (SAXS experimental data file) is expected as an input file."
        sys.exit(2) 
    
    
    pipelinelite = PipelineLite(datfile, output_path)
    pipelinelite.runPipeline()