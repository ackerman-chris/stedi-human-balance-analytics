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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1779744888177 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1779744888177")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1779744922776 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1779744922776")

# Script generated for node Join Step Trainer with Accelerometer
SqlQuery0 = '''
SELECT 
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
'''
JoinStepTrainerwithAccelerometer_node1779744942756 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1779744888177, "accelerometer_trusted":AccelerometerTrusted_node1779744922776}, transformation_ctx = "JoinStepTrainerwithAccelerometer_node1779744942756")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=JoinStepTrainerwithAccelerometer_node1779744942756, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779742259364", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1779744977581 = glueContext.getSink(path="s3://stedi-lakehouse-13055707/step_trainer/machine_learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1779744977581")
MachineLearningCurated_node1779744977581.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1779744977581.setFormat("json")
MachineLearningCurated_node1779744977581.writeFrame(JoinStepTrainerwithAccelerometer_node1779744942756)
job.commit()
