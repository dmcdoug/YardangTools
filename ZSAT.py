import arcpy

curvPolygs = arcpy.GetParameterAsText(0)

Input_Raster_or_Mosaic_Dataset = arcpy.GetParameterAsText(1)

output = arcpy.GetParameterAsText(2)

arcpy.sa.ZonalStatisticsAsTable(curvPolygs, "ORIG_FID", Input_Raster_or_Mosaic_Dataset, output, "DATA", "MIN_MAX_MEAN")

del curvPolygs, Input_Raster_or_Mosaic_Dataset, output