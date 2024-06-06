from pyspark.sql import SparkSession
from pyspark.sql.functions import col, format_number, concat, lit, avg, count, desc, sum as F_sum, round as spark_round, when, create_map, expr
from functools import reduce
import plotext as plt
from itertools import chain
from operator import itemgetter
from pyspark.sql.functions import count as spark_count

spark = SparkSession.builder.appName("AirbnbAnalysis").getOrCreate()
listingFile = spark.read.csv("ressources/listings.csv", header=True, inferSchema=True, multiLine=True, escape="\"")

total = listingFile.count()

room_type_counts = listingFile.groupBy('room_type').count()
room_type_counts = room_type_counts.withColumn('percentage', (col('count') / total) * 100)
room_type_counts = room_type_counts.withColumn('percentage', format_number('percentage', 1))
room_type_counts = room_type_counts.withColumn('percentage', concat(col('percentage'), lit('%')))
room_type_counts.show()

room_type_counts_data = room_type_counts.collect()

room_types = [row['room_type'] for row in room_type_counts_data]
counts = [row['count'] for row in room_type_counts_data]

plt.clear_data()
plt.bar(room_types, counts, orientation="horizontal", color=["red", "green", "blue", "orange"])
plt.xlabel("listings")
plt.ylabel("room type")
plt.title("Listings per Room Type")
plt.show()

listingFile = listingFile.withColumn("price", col("price").cast("string").substr(2, 100).cast("float"))
average_nights_booked = listingFile.agg(spark_round(avg(col("reviews_per_month") * col("minimum_nights") * 12), 0)).first()[0]
average_price_per_night = listingFile.agg(spark_round(avg("price"), 0)).first()[0]
average_income = listingFile.withColumn("estimated_income", col("reviews_per_month") * col("minimum_nights") * col("price") * 12) \
                            .agg(spark_round(avg("estimated_income"), 0)).first()[0]

print("\n")

print("Average nights booked:", average_nights_booked)
print("Price/night: £", average_price_per_night)
print("Average income: £", average_income)

print("\n")

total = listingFile.count()

listingFile = listingFile.withColumn("total_booked_nights", col("reviews_per_month") * col("minimum_nights") * 12)

bins = [0, 30, 60, 90, 120, 150, 180, 210, 240, 255]
labels = ["0-30", "31-60", "61-90", "91-120", "121-150", "151-180", "181-210", "211-240", "241-255+"]

label_order = {label: index for index, label in enumerate(labels)}

binned_data = listingFile.withColumn("occupancy_bin", 
    when(col("total_booked_nights") <= 30, labels[0])
    .when((col("total_booked_nights") > 30) & (col("total_booked_nights") <= 60), labels[1])
    .when((col("total_booked_nights") > 60) & (col("total_booked_nights") <= 90), labels[2])
    .when((col("total_booked_nights") > 90) & (col("total_booked_nights") <= 120), labels[3])
    .when((col("total_booked_nights") > 120) & (col("total_booked_nights") <= 150), labels[4])
    .when((col("total_booked_nights") > 150) & (col("total_booked_nights") <= 180), labels[5])
    .when((col("total_booked_nights") > 180) & (col("total_booked_nights") <= 210), labels[6])
    .when((col("total_booked_nights") > 210) & (col("total_booked_nights") <= 240), labels[7])
    .when(col("total_booked_nights") > 240, labels[8])
    .otherwise("Unknown"))

mapping_expr = create_map([lit(x) for x in chain(*label_order.items())])
binned_data = binned_data.withColumn("sort_order", mapping_expr[col("occupancy_bin")])

binned_data = binned_data.sort("sort_order")

occupancy_counts = binned_data.groupBy("occupancy_bin").count()

occupancy_counts_data = occupancy_counts.collect()

occupancy_counts_data = [row for row in occupancy_counts_data if row['occupancy_bin'] in label_order]

occupancy_counts_data = sorted(occupancy_counts_data, key=lambda row: label_order[row['occupancy_bin']])

bins = [row['occupancy_bin'] for row in occupancy_counts_data]
counts = [row['count'] for row in occupancy_counts_data]

plt.clear_data()
plt.bar(bins, counts, orientation="horizontal")
plt.xlabel("Listings")
plt.ylabel("Occupancy (Last 12 Months)")
plt.title("Distribution of Occupancy Over Last 12 Months")
plt.show()

print("\n")

short_term_listings = listingFile.filter(col("minimum_nights") <= 30).count()
long_term_listings = listingFile.filter(col("minimum_nights") > 30).count()

percentage_short_term = round((short_term_listings / total) * 100, 2)
percentage_long_term = round((long_term_listings / total) * 100, 2)

print("Short-term rentals: ", short_term_listings, "(", percentage_short_term, "%)")
print("Long-term rentals: ", long_term_listings, "(", percentage_long_term, "%)")

print("\n")

total = listingFile.count()

bins = list(range(1, 36)) + [float('inf')]
labels = [str(i) for i in range(1, 36)] + ["35+"]

nights_bin_expr = when(col("minimum_nights") == 1, "1")
for i in range(2, 36):
    nights_bin_expr = nights_bin_expr.when(col("minimum_nights") == i, str(i))
nights_bin_expr = nights_bin_expr.otherwise("35+")

listingFile = listingFile.withColumn("nights_bin", nights_bin_expr)

nights_counts = listingFile.groupBy("nights_bin").count()

nights_counts_data = nights_counts.collect()
label_order = {label: index for index, label in enumerate(labels)}
nights_counts_data = sorted(nights_counts_data, key=lambda row: label_order[row['nights_bin']])

bins = [row['nights_bin'] for row in nights_counts_data]
counts = [row['count'] for row in nights_counts_data]

plt.clear_data()
plt.bar(bins, counts, orientation="horizontal")
plt.xlabel("Listings")
plt.ylabel("Minimum Nights")
plt.title("Distribution of Minimum Nights")
plt.show()

print("\n")

total = listingFile.count()

total_hosts = listingFile.select("host_id").distinct().count()
single_listings = listingFile.filter(col("calculated_host_listings_count") == 1).select("host_id").distinct().count()
multi_listings = total_hosts - single_listings

percentage_single_listings = round((single_listings / total_hosts) * 100, 2)
percentage_multi_listings = round((multi_listings / total_hosts) * 100, 2)

print("Single listings: ", single_listings, "(", percentage_single_listings, "%)")
print("Multi-listings: ", multi_listings, "(", percentage_multi_listings, "%)")

labels = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10+"]
listing_counts = []

for i in range(1, 10):
    count = listingFile.filter(col("calculated_host_listings_count") == i).count()
    listing_counts.append(count)
listing_counts.append(listingFile.filter(col("calculated_host_listings_count") >= 10).count())

plt.clear_data()
plt.bar(labels, listing_counts, orientation="horizontal")
plt.xlabel("Listings per Host")
plt.ylabel("Listings")
plt.title("Listings per Host")
plt.show()

print("\n")

total = listingFile.count()

top_hosts = listingFile.groupBy("host_name", "room_type").agg(spark_count("room_type").alias("count"))
top_hosts = top_hosts.groupBy("host_name").pivot("room_type").sum("count")
top_hosts = top_hosts.withColumnRenamed("Entire home/apt", "#Entire home/apts") \
                     .withColumnRenamed("Private room", "#Private rooms") \
                     .withColumnRenamed("Shared room", "#Shared rooms") \
                     .withColumnRenamed("Hotel room", "#Hotel Rooms")
top_hosts = top_hosts.fillna(0)

columns_to_sum = [col(c) for c in top_hosts.columns if c != "host_name"]
top_hosts = top_hosts.withColumn("#Listings", reduce(lambda x, y: x + y, columns_to_sum))
top_hosts = top_hosts.sort(desc("#Listings"))

top_hosts.show()