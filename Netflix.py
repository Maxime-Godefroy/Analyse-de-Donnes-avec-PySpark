from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, split, explode, round, regexp_extract, avg, when, concat, lit, floor

spark = SparkSession.builder.appName("Netflix").getOrCreate()
netflix_titles = spark.read.csv("ressources/netflix_titles.csv", header=True, inferSchema=True)

netflix_titles = netflix_titles.withColumn("director", explode(split(col("director"), ", ")))

grouped_directors = netflix_titles.groupBy("director").agg(count("show_id").alias("nbr_films"))
sorted_directors = grouped_directors.sort(col("nbr_films").desc())

sorted_directors.show()


grouped_countries = netflix_titles.groupBy("country").agg(count("show_id").alias("nbr_productions"))

total_productions = netflix_titles.count()

grouped_countries_with_percentages = grouped_countries.withColumn("pourcentage", round((col("nbr_productions") / total_productions) * 100, 2))
sorted_countries_with_percentages = grouped_countries_with_percentages.sort(col("pourcentage").desc())

sorted_countries_with_percentages.show()


movies = netflix_titles.filter(col("type") == "Movie")
movies = movies.withColumn("duration_minutes", regexp_extract(col("duration"), r'(\d+)', 1).cast('int'))

average_duration = movies.agg(avg(col("duration_minutes")).alias("average_duration")).first()["average_duration"]
longest_movie = movies.orderBy(col("duration_minutes").desc()).select("title", "duration_minutes").first()
shortest_movie = movies.orderBy(col("duration_minutes").asc()).select("title", "duration_minutes").first()

print(f"Durée moyenne des films sur Netflix: {average_duration} minutes")
print(f"Le film le plus long sur Netflix est '{longest_movie['title']}' avec une durée de {longest_movie['duration_minutes']} minutes")
print(f"Le film le plus court sur Netflix est '{shortest_movie['title']}' avec une durée de {shortest_movie['duration_minutes']} minutes")


movies = netflix_titles.filter(col("type") == "Movie")
movies = movies.withColumn("duration_minutes", regexp_extract(col("duration"), r'(\d+)', 1).cast('int'))

movies = movies.withColumn("year_interval", 
                           concat((floor(col("release_year") / 2) * 2).cast("string"), 
                                  lit("-"), 
                                  (floor(col("release_year") / 2) * 2 + 1).cast("string")))
average_duration_by_interval = movies.groupBy("year_interval").agg(avg("duration_minutes").alias("average_duration")).sort(col("year_interval").desc())

average_duration_by_interval.show(truncate=False)


movies = netflix_titles.filter(col("type") == "Movie")
movies = movies.withColumn("director", explode(split(col("director"), ", "))) \
               .withColumn("actor", explode(split(col("cast"), ", ")))

director_actor_duo = movies.groupBy("director", "actor").agg(count("show_id").alias("nbr_collaborations"))
sorted_duo = director_actor_duo.sort(col("nbr_collaborations").desc())
top_duo = sorted_duo.limit(1)

top_duo.show(truncate=False)