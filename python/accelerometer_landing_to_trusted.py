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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1779743551347 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-13055707/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1779743551347")

# Script generated for node Customer Trusted
CustomerTrusted_node1779743587728 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1779743587728")

# Script generated for node Join with Trusted Customers
SqlQuery0 = '''
SELECT 
         a.timestamp,
         a.user,
         a.x,
         a.y,
         a.z
     FROM accelerometer_landing a
     INNER JOIN customer_trusted c
         ON a.user = c.email;
'''
JoinwithTrustedCustomers_node1779744041091 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1779743587728, "accelerometer_landing":AccelerometerLanding_node1779743551347}, transformation_ctx = "JoinwithTrustedCustomers_node1779744041091")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=JoinwithTrustedCustomers_node1779744041091, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779742259364", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1779744087403 = glueContext.getSink(path="s3://stedi-lakehouse-13055707/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1779744087403")
AccelerometerTrusted_node1779744087403.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1779744087403.setFormat("json")
AccelerometerTrusted_node1779744087403.writeFrame(JoinwithTrustedCustomers_node1779744041091)
job.commit()
