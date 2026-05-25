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

# Read step_trainer_landing from S3
StepTrainerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://YOUR-BUCKET-NAME/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node",
)

# Read customer_curated from Glue Catalog
CustomerCurated_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node",
)

# SQL Query: Inner join step_trainer with customer_curated on serialNumber
# Return only step_trainer columns
SqlQuery = """
SELECT DISTINCT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM step_trainer_landing s
INNER JOIN customer_curated c
    ON s.serialnumber = c.serialnumber
"""

StepTrainerTrusted_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "step_trainer_landing": StepTrainerLanding_node,
        "customer_curated": CustomerCurated_node,
    },
    transformation_ctx="StepTrainerTrusted_node",
)

# Write to S3 Trusted Zone
StepTrainerTrustedSink_node = glueContext.getSink(
    path="s3://YOUR-BUCKET-NAME/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrustedSink_node",
)
StepTrainerTrustedSink_node.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
StepTrainerTrustedSink_node.setFormat("json")
StepTrainerTrustedSink_node.writeFrame(StepTrainerTrusted_node)

job.commit()
