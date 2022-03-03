'''

    Title:

        multiexample

    Description:

        This simple script is the script that is wired up into toolbox

'''

import arcpy

import multicode

import daMerge, os

# Get parameters

curvPolygs = arcpy.GetParameterAsText(0)

Curvature_Raster = arcpy.GetParameterAsText(1)

Input_Raster_or_Mosaic_Dataset = arcpy.GetParameterAsText(2)

scratch = arcpy.GetParameterAsText(3)

output = arcpy.GetParameterAsText(4)

def main():
    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    arcpy.AddMessage("Calling multiprocessing code...")
    
    shpScratch = os.path.join(scratch.split('\\')[0],os.path.sep,*scratch.split('\\')[0:-1],'curvPolygs')

    results = multicode.multi(curvPolygs, Curvature_Raster, Input_Raster_or_Mosaic_Dataset, shpScratch)
    
    arcpy.AddMessage(str(results))
    
    arcpy.AddMessage("Merging Outputs")
    
    arcpy.env.workspace = shpScratch
    
    try:
    
        daMerge.daMerge(arcpy.ListFeatureClasses("curvPolyg_*","Polygon"),output)
        
    except:
    
        arcpy.AddMessage("daMerge failed, using builtin Merge tool")
        
        arcpy.management.Merge(arcpy.ListFeatureClasses("curvPolyg_*","Polygon"),output)

if __name__ == '__main__':

    main()
