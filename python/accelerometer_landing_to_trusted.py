import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Initialize Glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Helper function for SQL transforms
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Read accelerometer landing data from S3
AccelerometerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://YOUR-BUCKET-NAME/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node",
)

# Read customer_trusted data from Glue Catalog
CustomerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node",
)

# SQL Query: Inner join accelerometer with consenting customers
# Return only accelerometer columns (no customer PII)
SqlQuery = """
SELECT 
    a.timestamp,
    a.user,
    a.x,
    a.y,
    a.z
FROM accelerometer_landing a
INNER JOIN customer_trusted c
    ON a.user = c.email
"""

AccelerometerFiltered_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node,
        "customer_trusted": CustomerTrusted_node,
    },
    transformation_ctx="AccelerometerFiltered_node",
)

# Write to S3 Trusted Zone
AccelerometerTrusted_node = glueContext.getSink(
    path="s3://YOUR-BUCKET-NAME/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node",
)
AccelerometerTrusted_node.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node.setFormat("json")
AccelerometerTrusted_node.writeFrame(AccelerometerFiltered_node)

job.commit()
