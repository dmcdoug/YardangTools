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

# To allow overwriting outputs change overwriteOutput option to True.
arcpy.env.overwriteOutput = True

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

from ZonHistIterator_par import ZonHistIterator

def retryModel(elevLink, curvLink, curvPolygs_selection, thisScratch, curvPolygs, query, oid, numRetries = 3):
# Retries the model in case of random IO errors. Instead of numPolygs, outputs OID if it gives up.
    for attempt in range(numRetries):
        try:
            ValueHeight = ZonHistIterator(curvPolygs_selection, curvLink, elevLink, thisScratch)
            return ValueHeight        
        except Exception as e:
            #arcpy.management.Delete(curvPolygs_selection)
            if (attempt+1) < numRetries:
                continue #arcpy.analysis.Select(curvPolygs,curvPolygs_selection,query)
            elif (attempt+1) == numRetries:
                return str(oid)+": "+str(e)

def doWork(curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch, oid):

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
        # To allow overwriting outputs change overwriteOutput option to True.
        arcpy.env.overwriteOutput = True
        
        # Make, set new workspace
        thisScratch = os.path.join(shpScratch, shpScratch.split('\\')[-1]+'_'+str(oid))
        os.mkdir(thisScratch)
        
        curvLink = os.path.join(thisScratch,'c.tif')
        elevLink = os.path.join(thisScratch,'e.tif')
        try:
            os.symlink(Curvature_Raster,curvLink)
            os.symlink(Input_Raster_or_Mosaic_Dataset,elevLink)
        except:
            arcpy.AddError('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
            return str('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
        
        # Each curvPolygs layer needs a unique name, so use oid

        curvPolygs_selection = os.path.join(shpScratch,fr'curvPolyg_{str(oid)}.shp')

        # Select the polygon in the layer, this means the tool will use only that polygon

        #descObj = arcpy.Describe(curvPolygs)

        field = 'ORIG_FID' #descObj.OIDFieldName

        df = arcpy.AddFieldDelimiters(curvPolygs,field)

        query = df + " = " + str(oid)

        arcpy.analysis.Select(curvPolygs,curvPolygs_selection,query)

        # Do the thing
        #output = retryModel(elevLink, curvLink, curvPolygs_selection, thisScratch, curvPolygs, query, oid, numRetries = 3)
        #ValueHeight = ''
        #while type(ValueHeight).__name__=='str':
        output = ZonHistIterator(curvPolygs_selection, curvLink, elevLink, thisScratch)
            #if type(ValueHeight).__name__=='tuple' or ValueHeight == None:
                # break
        os.remove(curvLink)
        os.remove(elevLink)
        try:
            os.rmdir(thisScratch)
        except:
            pass
        
        try:
            # Remove these if using arcpy.SetLogMetadata(False) in ArcGIS Pro version >10.8
            os.remove(curvPolygs_selection+'.xml')
        except:
            pass
        
        return output

    except arcpy.ExecuteError:

        # Geoprocessor threw an error

        return arcpy.GetMessages(2)

    except Exception as e:

        # Capture all other errors

        return str(e)

def multi(curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch):
    
    # Check for symlink privileges and cancel run if lacking
    try:
        os.symlink(os.path.join(os.getcwd().split('\\')[0],os.path.sep,*os.getcwd().split('\\')[1:-1]),os.getcwd())
    except Exception as e:
        if str(e)=='symbolic link privilege not held':
            arcpy.AddError('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
            return str('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
    
    # Create folder to hold temp workspaces
    # shpScratch = os.path.join(scratch.split('\\')[0],os.path.sep,*scratch.split('\\')[0:-1],'curvPolygs')
    try:
        os.mkdir(shpScratch)
    except:
        arcpy.AddMessage(shpScratch + ' already exists')
        
    try:

        # To allow overwriting outputs change overwriteOutput option to True.
        arcpy.env.overwriteOutput = True
        
        # Create a list of object IDs for curvPolygs polygons

        arcpy.AddMessage("Creating Polygon OID list...")

        #descObj = arcpy.Describe(curvPolygs)

        field = 'ORIG_FID' #descObj.OIDFieldName

        idList = []

        with arcpy.da.SearchCursor(curvPolygs,[field]) as cursor:

            for row in cursor:

                id = row[0]

                idList.append(id)

        arcpy.AddMessage("There are " + str(len(idList)) + " object IDs (polygons) to process.")        

        # Call doWork function, this function is called as many OIDS in idList

        # This line creates a "pointer" to the real function but its a nifty way for declaring parameters.

        func = partial(doWork,curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch)

        arcpy.AddMessage("Sending to pool")

        # declare number of cores to use, use 1 less than the max

        cpuNum = multiprocessing.cpu_count() - 1

        # Create the pool object

        pool = multiprocessing.Pool(processes=cpuNum)#,maxtasksperchild=1)

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
        
def single(curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch):
    
    # Check for symlink privileges and cancel run if lacking
    try:
        os.symlink(os.path.join(os.getcwd().split('\\')[0],os.path.sep,*os.getcwd().split('\\')[1:-1]),os.getcwd())
    except Exception as e:
        if str(e)=='symbolic link privilege not held':
            arcpy.AddError('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
            return str('Failed to create symlink necessary for parallel processing. Rerun everything as an Administrator.')
    
    # Create folder to hold temp workspaces
    # shpScratch = os.path.join(scratch.split('\\')[0],os.path.sep,*scratch.split('\\')[0:-1],'curvPolygs')
    try:
        os.mkdir(shpScratch)
    except:
        arcpy.AddMessage(shpScratch + ' already exists')
    results = []
    field = 'ORIG_FID'
    with arcpy.da.SearchCursor(curvPolygs,[field]) as cursor:
        for row in cursor:
            result = doWork(curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch, row[0])
            arcpy.AddMessage(result)
            results += result
    return results