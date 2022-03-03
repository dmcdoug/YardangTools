'''

    Title:

        multiexample

    Description:

        This simple script is the script that is wired up into toolbox

'''

import arcpy, os

import daMerge

# Disable large xml log files for each shapefile. Requires ArcGIS Pro >10.8
try:
    if arcpy.GetLogMetadata():
        arcpy.SetLogMetadata(False)
    if arcpy.GetLogHistory():
        arcpy.SetLogHistory(False)
except:
        pass

import multicode_eP

# Get parameters

Input_Raster = arcpy.GetParameterAsText(0)

seedpoints = arcpy.GetParameterAsText(1)

Scratch = arcpy.GetParameterAsText(2)

Watersheds = arcpy.GetParameterAsText(3)

eP_output = arcpy.GetParameterAsText(4)

sP_output = arcpy.GetParameterAsText(5)

# eP_output = os.path.join(Scratch,'elevPolygs_raw')

# arcpy.SetParameterAsText(4, eP_output)

# sP_output = os.path.join(Watersheds.split('\\')[0],os.path.sep,*Watersheds.split('\\')[0:-1],'seedpoints_near')

# arcpy.SetParameterAsText(5, sP_output)

def main():
    # To allow overwriting outputs change overwriteOutput option to True.
    arcpy.env.overwriteOutput = True

    arcpy.AddMessage("Calling multiprocessing code...")

    results = multicode_eP.multi(Input_Raster, seedpoints, Scratch, Watersheds)
    
    arcpy.AddMessage(str(results))
    
    shpScratch = os.path.join(Scratch.split('\\')[0],os.path.sep,*Scratch.split('\\')[0:-1],'elevPolygs')
    
    arcpy.AddMessage("Merging Outputs")
    
    arcpy.env.workspace = shpScratch
    
    try:
        
        daMerge.daMerge(arcpy.ListFeatureClasses("elevPolyg_*","Polygon"),eP_output)
  
    except:
    
        arcpy.AddMessage("daMerge failed, using builtin Merge tool")
        
        arcpy.management.Merge(arcpy.ListFeatureClasses("elevPolyg_*","Polygon"),eP_output)
        
    try: # I added Near fields to all elevpolygs, so daMerge should work now
        
        daMerge.daMerge(arcpy.ListFeatureClasses("seedpoint_*","Point"),sP_output)
  
    except:
    
        arcpy.AddMessage("daMerge failed, using builtin Merge tool")
        
        arcpy.management.Merge(arcpy.ListFeatureClasses("seedpoint_*","Point"),sP_output)
        


if __name__ == '__main__':

    main()