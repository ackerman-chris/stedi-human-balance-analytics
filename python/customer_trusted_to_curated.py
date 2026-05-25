import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1779744408808 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1779744408808")

# Script generated for node Customer Trusted
CustomerTrusted_node1779744346804 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerTrusted_node1779744346804")

# Script generated for node Join Customer with Accelerometer
SqlQuery0 = '''
SELECT DISTINCT
         c.serialnumber,
         c.sharewithpublicasofdate,
         c.birthday,
         c.registrationdate,
         c.sharewithresearchasofdate,
         c.customername,
         c.email,
         c.lastupdatedate,
         c.phone,
         c.sharewithfriendsasofdate
     FROM customer_trusted c
     INNER JOIN accelerometer_trusted a
         ON c.email = a.user
'''
JoinCustomerwithAccelerometer_node1779744370590 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1779744346804, "accelerometer_trusted":AccelerometerTrusted_node1779744408808}, transformation_ctx = "JoinCustomerwithAccelerometer_node1779744370590")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=JoinCustomerwithAccelerometer_node1779744370590, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779742259364", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1779744457570 = glueContext.getSink(path="s3://stedi-lakehouse-13055707/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1779744457570")
CustomerCurated_node1779744457570.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1779744457570.setFormat("json")
CustomerCurated_node1779744457570.writeFrame(JoinCustomerwithAccelerometer_node1779744370590)
job.commit()
