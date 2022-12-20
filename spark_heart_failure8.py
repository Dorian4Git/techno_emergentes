# Import necessary libraries
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
import pandas as pd

# Create a SparkSession
spark = SparkSession.builder.appName("Heart Failure Analysis").enableHiveSupport(
).getOrCreate()

# Set the log level to ERROR
spark.sparkContext.setLogLevel("ERROR")
print("Logging level set to ERROR")

# Read the JSON file into a DataFrame
df = spark.read.json("output/data_consumed.json")

# Write the DataFrame to a Hive table
df.write.format("hive").mode("overwrite").saveAsTable("heart_failure_records")
print("Wrote heart_failure_records to Hive")

# print the schema of the DataFrame
print("DataFrame schema:")
df.printSchema()

# Create a temporary view of the data
df.createOrReplaceTempView("heart_failure_view")
print("Created temporary view heart_failure_view")

# write a line of characters at the beginning of the file
with open("output/tables.txt", "w") as f:  # use "w" mode to overwrite the file
    f.write("#" * 80 + "\n")  # write 80 "#" characters followed by a newline
    f.write("\t\t\tTables from the spark analysis:\n")
    f.write("#" * 80 + "\n")  # write another line of 80 "#" characters

# ======================
# Write a query to count the number of records
# ======================
count_query = "SELECT COUNT(*) FROM heart_failure_view"
count_df = spark.sql(count_query)

# show the number of records
print("\nNumber of records:")
count_df.show()

# save as a Hive table
count_df.write.format("hive").mode("overwrite").saveAsTable("record_count")
print("Wrote record_count to Hive")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write("\nNumber of rows in the heart_failure_view table:\n")
    f.write("count\n")  # write the header row
    for row in count_df.collect():
        f.write(str(row[0]) + "\n")  # write the count value
print("Wrote record_count to file")

# ======================
# Write a query to count the number of records by DEATH_EVENT
# ======================
death_event_count_query = "SELECT DEATH_EVENT, COUNT(*) AS count FROM heart_failure_view GROUP BY DEATH_EVENT"
death_event_count_df = spark.sql(death_event_count_query)

# show the number of records by DEATH_EVENT
print("\nNumber of records by DEATH_EVENT:")
death_event_count_df.show()

# save as a Hive table
death_event_count_df.write.format("hive").mode(
    "overwrite").saveAsTable("death_event_count")
print("Wrote death_event_count to Hive")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write("\nNumber of rows in each DEATH_EVENT group:\n")
    f.write("DEATH_EVENT\tcount\n")  # write the header row
    for row in death_event_count_df.collect():
        # write each row with tab-separated values
        f.write(str(row.DEATH_EVENT) + "\t" + str(row.count) + "\n")
print("Wrote death_event_count to file")

# ======================
# Write a query to find the average age of patients who experienced a death event
# ======================
avg_age_query = "SELECT AVG(age) FROM heart_failure_view WHERE DEATH_EVENT = 1"
avg_age_df = spark.sql(avg_age_query)

# show the average age of patients who experienced a death event
print("\nAverage age of patients who experienced a death event:")
avg_age_df.show()

# save as a Hive table
avg_age_df.write.format("hive").mode(
    "overwrite").saveAsTable("avg_age_death_event")
print("Wrote avg_age_death_event to Hive")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write("\nAverage age of individuals with DEATH_EVENT = 1:\n")
    f.write("avg_age\n")  # write the header row
    for row in avg_age_df.collect():
        f.write(str(row[0]) + "\n")  # write the average age
print("Wrote avg_age_death_event to file")


# ======================
# create a table with the average ejection fraction of individuals who died from a heart disease, grouped by high blood pressure status
# ======================
print("\nAverage ejection fraction of individuals who died from a heart disease, grouped by high blood pressure status:")
death_ef_bp = spark.sql("""
    SELECT high_blood_pressure, AVG(ejection_fraction) AS avg_ef
    FROM heart_failure_view
    WHERE DEATH_EVENT = '1'
    GROUP BY high_blood_pressure
""")
death_ef_bp.show()

# save as a Hive table
death_ef_bp.write.format("hive").mode("overwrite").saveAsTable("death_ef_bp")
print("Wrote death_ef_bp to Hive")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write("\nAverage ejection fraction of individuals who died from a heart disease, grouped by high blood pressure status:\n")
    f.write("high_blood_pressure\tavg_ef\n")  # write the header row
    for row in death_ef_bp.collect():
        # write each row with tab-separated values
        f.write(str(row.high_blood_pressure) + "\t" + str(row.avg_ef) + "\n")
print("Wrote death_ef_bp to file")

# ======================
# Write a query to find the most predictive variable for death events
# ======================
prediction_query = """
SELECT variable, COUNT(*) AS count
FROM (
  SELECT
    CASE
      WHEN age = 1 THEN 'age'
      WHEN anaemia = 1 THEN 'anaemia'
      WHEN creatinine_phosphokinase = 1 THEN 'creatinine_phosphokinase'
      WHEN diabetes = 1 THEN 'diabetes'
      WHEN ejection_fraction = 1 THEN 'ejection_fraction'
      WHEN high_blood_pressure = 1 THEN 'high_blood_pressure'
      WHEN platelets = 1 THEN 'platelets'
      WHEN serum_creatinine = 1 THEN 'serum_creatinine'
      WHEN serum_sodium = 1 THEN 'serum_sodium'
      WHEN sex = 1 THEN 'sex'
      WHEN smoking = 1 THEN 'smoking'
      ELSE 'other'
    END AS variable
  FROM heart_failure_view
  WHERE DEATH_EVENT = 1
) t
GROUP BY variable
ORDER BY count DESC
LIMIT 1
"""
prediction_df = spark.sql(prediction_query)

# show predictive variables
print("\nMost predictive variable for death events:")
prediction_df.show()

# Text description of anaemia
print("Anaemia: Decrease of red blood cells or hemoglobin")

# save as a Hive table
prediction_df.write.format("hive").mode(
    "overwrite").saveAsTable("most_predictive_variable")
print("Wrote most_predictive_variable to Hive")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write("\nMost predictive variable for death events:\n")
    f.write("variable\tcount\n")  # write the header row
    for row in prediction_df.collect():
        # write each row with tab-separated values
        f.write(row.variable + "\t" + str(row.count) + "\n")
print("Wrote most_predictive_variable to file")

# ======================
# create a table with the average age of individuals who died from a heart disease, grouped by sex
# ======================
print("\nAverage age of individuals who died from a heart disease, grouped by sex:")
death_age_sex = spark.sql("""
    SELECT sex, AVG(age) AS avg_age
    FROM heart_failure_records
    WHERE DEATH_EVENT = '1'
    GROUP BY sex
""")
death_age_sex.show()

# save the table to "output/tables.txt"s
# death_age_sex.coalesce(1).write.format("csv").save(
#     "output/death_age_sex.csv", mode="overwrite")

# write the table to a file
with open("output/tables.txt", "a") as f:
    f.write(
        "\nAverage age of individuals who died from a heart disease, grouped by sex:\n")
    f.write("sex\tavg_age\n")  # write the header row
    for row in death_age_sex.collect():
        # write each row with tab-separated values
        f.write(str(row.sex) + "\t" + str(row.avg_age) + "\n")
print("Wrote average age of individuals who died from a heart disease, grouped by sex to file\n")

# ======================
# Prediction ML
# ======================
print("\nPrediction ML:\n")
# Split the data into training and test sets
(train_df, test_df) = df.randomSplit([0.7, 0.3], seed=123)

# Define the features and label
feature_cols = ['age', 'anaemia', 'creatinine_phosphokinase', 'diabetes', 'ejection_fraction',
                'high_blood_pressure', 'platelets', 'serum_creatinine', 'serum_sodium', 'sex', 'smoking']
label_col = 'DEATH_EVENT'


# Build the model using a logistic regression algorithm
va = VectorAssembler(inputCols=feature_cols, outputCol="features")
# df = va.transform(df)
train_df = va.transform(train_df)
test_df = va.transform(test_df)

lr = LogisticRegression(featuresCol="features", labelCol=label_col)
# lr = LogisticRegression(featuresCol=','.join(feature_cols), labelCol=label_col)
# lr = LogisticRegression(featuresCol=feature_cols, labelCol=label_col)
model = lr.fit(train_df)

# Make predictions on the test set
predictions_df = model.transform(test_df)

# Evaluate the model using a binary classification evaluator
evaluator = BinaryClassificationEvaluator(labelCol=label_col)
accuracy = evaluator.evaluate(
    predictions_df, {evaluator.metricName: "areaUnderROC"})

# Select the features, label, and prediction columns
predictions_df = predictions_df.select(
    feature_cols + [label_col, 'prediction'])

# Print the predictions
predictions_df.show()

# Convert the DataFrame to a Pandas DataFrame
predictions_df_pandas = predictions_df.toPandas()

# Write the predictions to a CSV file separated by tabs
predictions_df_pandas.to_csv('output/predictions.txt', sep='\t', index=False)

# Print the accuracy
print("\nAccuracy:", accuracy)

# ======================
# count the number of individuals in the dataset
# ======================
print("\nNumber of individuals in the dataset: ")
print(df.select("*").count())

# Stop the SparkSession
spark.stop()
