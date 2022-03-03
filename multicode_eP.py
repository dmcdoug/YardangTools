"""

    Title:

        multicode

    Description:

        The module that does multicore zonal histogram iteration of yardang curvpolygs

    Limitations:

        As geoprocessing objects cannot be "pickled" the full path to the dataset is passed to the worker function. This means that any selection on the input curvPolygs layer is ignored.

    Author:

        Duncan Hornby (ddh@geodata.soton.ac.uk)        https://community.esri.com/t5/python-documents/create-a-script-tool-that-uses-multiprocessing/ta-p/913862

    Created:

        2/4/15
    
    Edited: 
    
        Bridger Swenson, Dylan McDougall, Summer 2021

"""

import os, sys, arcpy, multiprocessing

# Disable large xml log files for each shapefile. Requires ArcGIS Pro >10.8
try:
    if arcpy.GetLogMetadata():
        arcpy.SetLogMetadata(False)
    if arcpy.GetLogHistory():
        arcpy.SetLogHistory(False)
except:
        pass
    
multiprocessing.set_executable(os.path.join(sys.exec_prefix, 'pythonw.exe')) 
#multiprocessing.set_start_method("spawn")

from functools import partial

from Model_redo4_par import Model1211

# To allow overwriting outputs change overwriteOutput option to True.
arcpy.env.overwriteOutput = True

def retryModel(elevLink, seedpoint, elevPolyg_3_, thisScratch, Watersheds, seedpoints, query, oid, numRetries = 3):
# Retries the model in case of random IO errors. Instead of numPolygs, outputs OID if it gives up.
    for attempt in range(numRetries):
        try:
            numPolygs = Model1211(elevLink, seedpoint, elevPolyg_3_, thisScratch, Watersheds)
            return numPolygs        
        except Exception as e:
            arcpy.management.Delete(seedpoint)
            if (attempt+1) < numRetries:
                arcpy.analysis.Select(seedpoints,seedpoint,query)
            elif (attempt+1) == numRetries:
                return str(oid)+": "+str(e)
                
def doWork(Input_Raster, seedpoints, shpScratch, Watersheds, oid):

    """

        Title:

            doWork

        Description:

            This is the function that gets called as does the work. The parameter oid comes from the idList when the

            function is mapped by pool.map(func,idList) in the multi function.

            Note that this function does not try to write to arcpy.AddMessage() as nothing is ever displayed.

            If the tool succeeds then it returns TRUE else FALSE.

    """

    try:
        # Make, set new workspace
        thisScratch = os.path.join(shpScratch, shpScratch.split('\\')[-1]+'_'+str(oid))
        os.mkdir(thisScratch)
        
        elevLink = os.path.join(thisScratch,'e.tif')
        try:
            os.symlink(Input_Raster,elevLink)
        except:
            arcpy.AddMessage('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
            return 'Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.'
        
        # Each layer needs a unique name, so use oid

        seedpoint = os.path.join(shpScratch,fr'seedpoint_{str(oid)}.shp')
        elevPolyg_3_ = os.path.join(shpScratch,fr'elevPolyg_{str(oid)}.shp')

        # Select the point in the layer, this means the tool will use only that point

        field = 'ORIG_FID'

        df = arcpy.AddFieldDelimiters(seedpoints,field)

        query = df + " = " + str(oid)

        arcpy.analysis.Select(seedpoints,seedpoint,query)

        # Do the thing
        # numPolygs = ''
        # while type(numPolygs).__name__=='str':
            # numPolygs = Model1211(elevLink, seedpoint, elevPolyg_3_, thisScratch, Watersheds)
        output = retryModel(elevLink, seedpoint, elevPolyg_3_, thisScratch, Watersheds, seedpoints, query, oid)
        os.remove(elevLink)
        try:
            os.rmdir(thisScratch)
        except:
            pass
        
        try:
            # Remove these if using arcpy.SetLogMetadata(False) in ArcGIS Pro version >10.8
            if output>0:
                os.remove(os.path.join(shpScratch,fr'seedpoint_{str(oid)}.shp.xml'))
                os.remove(os.path.join(shpScratch,fr'elevPolyg_{str(oid)}.shp.xml'))
        except:
            pass
        
        return output

    except arcpy.ExecuteError:

        # Geoprocessor threw an error

        return arcpy.GetMessages(2)

    except Exception as e:

        # Capture all other errors

        return str(e)

def multi(Input_Raster, seedpoints, Scratch, Watersheds):
    
    # Create folder to hold temp workspaces
    shpScratch = os.path.join(Scratch.split('\\')[0],os.path.sep,*Scratch.split('\\')[0:-1],'elevPolygs')
    try:
        os.mkdir(shpScratch)
    except:
        arcpy.AddMessage(shpScratch + ' already exists')
        
    try:

        arcpy.env.overwriteOutput = True
        
        # Create a list of object IDs

        arcpy.AddMessage("Creating Polygon OID list...")

        field = 'ORIG_FID' #descObj.OIDFieldName

        idList = []

        with arcpy.da.SearchCursor(seedpoints,[field]) as cursor:

            for row in cursor:

                id = row[0]

                idList.append(id)

        arcpy.AddMessage("There are " + str(len(idList)) + " object IDs to process.")        

        # Call doWork function, this function is called as many OIDS in idList

        # This line creates a "pointer" to the real function but its a nifty way for declaring parameters.

        func = partial(doWork,Input_Raster, seedpoints, shpScratch, Watersheds)

        arcpy.AddMessage("Sending to pool")

        # declare number of cores to use, use 1 less than the max

        cpuNum = multiprocessing.cpu_count() - 1
        
        arcpy.AddMessage("Sending to pool with "+str(cpuNum)+" workers")

        # Create the pool object

        pool = multiprocessing.Pool(processes=cpuNum)

        # Fire off list to worker function.

        # res is a list that is created with whatever the worker function is returning

        results = pool.map(func,idList) #.map_async
        #res = [result for result in result]
        
        # arcpy.AddMessage(str(res))

        pool.close()

        pool.join()
        
        arcpy.AddMessage("Finished multiprocessing!")
        
        return results

    except arcpy.ExecuteError:

        # Geoprocessor threw an error

        arcpy.AddError(arcpy.GetMessages(2))

    except Exception as e:

        # Capture all other errors

        arcpy.AddError(str(e))
        
