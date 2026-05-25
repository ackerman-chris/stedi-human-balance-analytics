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

# Read customer landing data from S3
CustomerLanding_node = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://YOUR-BUCKET-NAME/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node",
)

# SQL Query: Filter only customers who agreed to share data for research
SqlQuery = """
SELECT *
FROM customer_landing
WHERE sharewithresearchasofdate IS NOT NULL
"""

CustomerFiltered_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={"customer_landing": CustomerLanding_node},
    transformation_ctx="CustomerFiltered_node",
)

# Write to S3 Trusted Zone
CustomerTrusted_node = glueContext.getSink(
    path="s3://YOUR-BUCKET-NAME/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node",
)
CustomerTrusted_node.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
CustomerTrusted_node.setFormat("json")
CustomerTrusted_node.writeFrame(CustomerFiltered_node)

job.commit()
