import arcpy, os
from arcpy import env  
from arcpy.sa import Idw, ExtractByMask




def CreateTiff(inShapefile, fieldName, maskShapefile, outRaster):
    """"

    """
    outIDW = Idw(inShapefile, fieldName, 500, 2, arcpy.sa.RadiusVariable(8))
    outExtractByMask = ExtractByMask(outIDW, maskShapefile)
    arcpy.RasterToOtherFormat_conversion(outExtractByMask, outRaster, "TIFF")
    #outExtractByMask.save(outShapefile) # "C:/sapyexamples/output/idwout.tif"



workingDirectory = r"E:\git\sage_spatial_analysis\comparison_manuscript\filters_v3"
outTiffDirectory =r"E:\git\sage_spatial_analysis\comparison_manuscript\tiff_v3"
maskShapefilePath  = r"E:\sage\mn_boundary_26915.shp"
fieldName = "ratio"
env.workspace = workingDirectory


for aFC in arcpy.ListFeatureClasses("*","point"):
    outTiffName = aFC.split(".")[0]
    outTiffFilePath = os.path.join( outTiffDirectory,".".join([outTiffName,"tiff"]) )
    print(aFC, fieldName, maskShapefilePath, outTiffFilePath)
    CreateTiff(aFC, fieldName, maskShapefilePath, outTiffDirectory)