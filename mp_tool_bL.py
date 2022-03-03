'''

    Title:

        multiexample

    Description:

        This simple script is the script that is wired up into toolbox

'''

import arcpy

import multicode_bL

# Get parameters

Watersheds = arcpy.GetParameterAsText(0)
elevPolygs = arcpy.GetParameterAsText(1)
output = arcpy.GetParameterAsText(2)

def main():

    shpScratch = os.path.join(output.split('\\')[0],os.path.sep,*output.split('\\')[0:-2],'temp')

    arcpy.AddMessage("Calling multiprocessing code...")

    results = multicode_bL.multi(shpScratch, Watersheds, elevPolygs, output)
    
    arcpy.AddMessage(str(results))
    
    arcpy.AddMessage("Merging Outputs")
    
    try:    
        arcpy.env.workspace = shpScratch
        
        arcpy.management.Merge(arcpy.ListFeatureClasses("clip_*","Polyline"),output)
        
    except arcpy.ExecuteError:

        # Geoprocessor threw an error

        arcpy.AddError(arcpy.GetMessages(2))

    except Exception as e:

        # Capture all other errors

        arcpy.AddError(str(e))

if __name__ == '__main__':

    main()