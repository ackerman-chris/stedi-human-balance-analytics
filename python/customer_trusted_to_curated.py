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

# Read customer_trusted from Glue Catalog
CustomerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node",
)

# Read accelerometer_trusted from Glue Catalog
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# SQL Query: Inner join + DISTINCT to get only customers with accelerometer data
SqlQuery = """
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
"""

CustomerCurated_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "customer_trusted": CustomerTrusted_node,
        "accelerometer_trusted": AccelerometerTrusted_node,
    },
    transformation_ctx="CustomerCurated_node",
)

# Write to S3 Curated Zone
CustomerCuratedSink_node = glueContext.getSink(
    path="s3://YOUR-BUCKET-NAME/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCuratedSink_node",
)
CustomerCuratedSink_node.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
CustomerCuratedSink_node.setFormat("json")
CustomerCuratedSink_node.writeFrame(CustomerCurated_node)

job.commit()
