from pyspark.sql.functions import mean,stddev  #Import functions for average and std

file_path = df ='dbfs:/FileStore/tables/annual_enterprise_survey_2018_financial_year_provisional_csv-40b11.csv'
df = spark.read.csv(file_path, header="true", inferSchema="true")

dbutils.fs.put("newDir/count.txt", "Count:") #Run once to create file (newDir is my Dir in databricks)

count = df.count()

with open("/dbfs/newDir/count.txt", 'a+') as f: 
  f.write("{}\n".format(count))
f.close()

with open("/dbfs/newDir/count.txt", 'r') as f:
  print(f.read())
f.close()

print(df.columns)

df.select("Year").rdd.max()[0]  #Get max
df.select("Year").rdd.min()[0]  #Get min
df.select(mean("Year")).show()  #Average
df.select(stddev("Year")).show() #std

path_to_save_file = "dbfs:/FileStore/tables/coalesce"

df.write.format("csv").mode("overwrite").save(path_to_save_file)