#Written by Chris Snyder, modified by DSM to merge shapefiles to gdb feature class. 48% faster than arcpy.management.Merge (as of Pro 2.6.3)
#Source: https://community.esri.com/t5/python-questions/a-better-way-to-run-large-append-merge-jobs/td-p/346209

import arcpy

def daMerge(fcList,outputFC,fields = ''):
    # try:
    if fields == '':
        fields = [f.name for f in arcpy.ListFields(fcList[0])] # necessary for copying from shapefiles to feature class 
    for fc in fcList:
        if fcList.index(fc) == 0:
            arcpy.CopyFeatures_management(fc, outputFC)
            insertRows = arcpy.da.InsertCursor(outputFC, ['SHAPE@','OBJECTID', *fields[1:]])
        else:
            searchRows = arcpy.da.SearchCursor(fc, ['SHAPE@','FID', *fields[1:]])
            for searchRow in searchRows:
                insertRows.insertRow(searchRow)
            del searchRow, searchRows
    del insertRows
    # except Exception as e:

        # # Capture all other errors

        # arcpy.AddMessage(str(e))
        # arcpy.AddMessage(['daMerge failed on fc #' + str(fcList.index(fc))])