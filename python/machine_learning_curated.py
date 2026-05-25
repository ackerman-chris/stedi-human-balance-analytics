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

# Read step_trainer_trusted from Glue Catalog
StepTrainerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node",
)

# Read accelerometer_trusted from Glue Catalog
AccelerometerTrusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node",
)

# SQL Query: Inner join step_trainer with accelerometer on timestamp
SqlQuery = """
SELECT DISTINCT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.user,
    a.x,
    a.y,
    a.z,
    a.timestamp
FROM step_trainer_trusted s
INNER JOIN accelerometer_trusted a
    ON s.sensorreadingtime = a.timestamp
"""

MachineLearningCurated_node = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node,
        "accelerometer_trusted": AccelerometerTrusted_node,
    },
    transformation_ctx="MachineLearningCurated_node",
)

# Write to S3 Curated Zone
MachineLearningSink_node = glueContext.getSink(
    path="s3://YOUR-BUCKET-NAME/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningSink_node",
)
MachineLearningSink_node.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningSink_node.setFormat("json")
MachineLearningSink_node.writeFrame(MachineLearningCurated_node)

job.commit()
