from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, round, when, month, year, lit, concat

spark = SparkSession.builder.appName("WordCount").getOrCreate()
listingFile = spark.read.csv("ressources/trump_insult_tweets_2014_to_2021.csv", header=True, inferSchema=True, multiLine=True, escape="\"")


grouped_data = listingFile.groupBy("target").agg(count("insult").alias("nrb-insultes"))
sorted_data = grouped_data.sort(col("nrb-insultes").desc())

top_7_insulted = sorted_data.limit(7)
total_insults = listingFile.count()

top_7_insulted_with_percentages = top_7_insulted.withColumn("pourcentage", round((col("nrb-insultes") / total_insults) * 100, 2))

top_7_insulted_with_percentages.show()


grouped_insults = listingFile.groupBy("insult").agg(count("insult").alias("nrb-insultes"))
sorted_insults = grouped_insults.sort(col("nrb-insultes").desc())

top_7_insults = sorted_insults.limit(7)
total_insults = listingFile.count()

top_7_insults_with_percentages = top_7_insults.withColumn("pourcentage", round((col("nrb-insultes") / total_insults) * 100, 2))

top_7_insults_with_percentages.show()



biden_insults = listingFile.filter(col("target") == "joe-biden")
grouped_insults = biden_insults.groupBy("insult").agg(count("insult").alias("nrb-insultes"))

sorted_insults = grouped_insults.sort(col("nrb-insultes").desc())
top_insult = sorted_insults.limit(1)

top_insult.show()



mexico_tweets = listingFile.filter(col("tweet").contains("Mexico")).count()
china_tweets = listingFile.filter(col("tweet").contains("China")).count()
coronavirus_tweets = listingFile.filter(col("tweet").contains("Coronavirus")).count()

print(f"Nombre de tweets contenant 'Mexico': {mexico_tweets}")
print(f"Nombre de tweets contenant 'China': {china_tweets}")
print(f"Nombre de tweets contenant 'Coronavirus': {coronavirus_tweets}")

print("\n")

listingFile = listingFile.withColumn("date_range", 
                              when(month(col("date")).between(1, 6), 
                                  concat(year(col("date")), lit("/01"), lit(" - "), year(col("date")), lit("/06")))
                              .otherwise(concat(year(col("date")), lit("/07"), lit(" - "), year(col("date")), lit("/12"))))
grouped_data = listingFile.groupBy("date_range").agg(count("tweet").alias("nrb-tweets"))

sorted_data = grouped_data.sort(col("date_range"))

sorted_data.show()