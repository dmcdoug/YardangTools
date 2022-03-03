import arcpy, daMerge
arcpy.env.overwriteOutput = True

ws = arcpy.GetParameterAsText(0)
name = arcpy.GetParameterAsText(1)
out = arcpy.GetParameterAsText(2)
fields = arcpy.GetParameterAsText(3)

# def daMergeTool():
arcpy.env.workspace = ws#os.path.join(ws.split('\\')[0],os.path.sep,*ws.split('\\')[0:-1])
    # try:
daMerge.daMerge(arcpy.ListFeatureClasses(name),out,fields)
    #ws.split('\\')[-1]),out)
    # except arcpy.ExecuteError:
        # # Geoprocessor threw an error

        # arcpy.AddError(arcpy.GetMessages(2))
        # arcpy.AddMessage(['daMerge failed on fc #' + str(fcList.index(fc))])

    # except Exception as e:
        # # Capture all other errors

        # arcpy.AddError(str(e))
        # arcpy.AddMessage(['daMerge failed on fc #' + str(fcList.index(fc))])
        
# if __name__ == '__main__':
    # daMergeTool()