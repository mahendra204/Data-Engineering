# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

IPL_auction_data=spark.read.csv("dbfs:/FileStore/tables/IPL_auction_data.csv", inferSchema=False,header=True)

# COMMAND ----------

IPL_auction_data.show()

# COMMAND ----------

IPL_auction_data = IPL_auction_data.withColumn("Base price", F.col("Base price").cast("int"))
IPL_auction_data = IPL_auction_data.withColumn("Winning bid", F.col("Winning bid").cast("int"))
IPL_auction_data = IPL_auction_data.withColumn("Year", F.col("Year").cast("int"))

# COMMAND ----------

IPL_auction_data.printSchema()

# COMMAND ----------

len(IPL_auction_data.columns)

# COMMAND ----------

IPL_auction_data.count()

# COMMAND ----------

IPL_auction_data = IPL_auction_data.withColumnRenamed("_c0", "S.No")

# COMMAND ----------

IPL_auction_data.show()

# COMMAND ----------

ipl2023auction=IPL_auction_data.where(F.col('Year') == 2023)

# COMMAND ----------

ipl2023auction.show()

# COMMAND ----------

ipl2023auction.count()

# COMMAND ----------

ipl2023auction.count()

# COMMAND ----------

avg_winning_bid_by_country = IPL_auction_data.where(F.col('Year') == 2023).groupBy('Country').agg(F.count('Player').alias('no.of_players'))

# COMMAND ----------

avg_winning_bid_by_country.show()

# COMMAND ----------

window_spec = Window().partitionBy('Year').orderBy(F.col('Winning bid').desc())
ranked_players = IPL_auction_data.withColumn('BidRank', F.rank().over(window_spec))

# COMMAND ----------

ranked_players.show()

# COMMAND ----------

ipl2023auction.printSchema()

# COMMAND ----------

window_spec = Window().partitionBy('Year').orderBy(F.col('Winning bid').desc())
ranked_players = ipl2023auction.withColumn('BidRank', F.dense_rank().over(window_spec))

# COMMAND ----------

ranked_players.show()

# COMMAND ----------

avg_winning_bid_by_country = ipl2023auction.groupBy('Country').agg(F.round(F.avg('Winning bid'),2).alias('avg_winning_bid'),F.count('Player').alias("no_of_players"))

# COMMAND ----------

avg_winning_bid_by_country.show()

# COMMAND ----------

total_bid_amount = avg_winning_bid_by_country.withColumn('total_winning_bid', F.round(F.col('avg_winning_bid') * F.col('no_of_players'), 2)).orderBy(F.col("total_winning_bid").desc())

# COMMAND ----------

result_with_product.show()

# COMMAND ----------

percentage_increase = ipl2023auction.withColumn('PercentageIncrease', ((F.col('Winning bid') - F.col('Base price')) / F.col('Base price')) * 100).orderBy(F.col("PercentageIncrease").desc()).show()

# COMMAND ----------

max_winning_bid_players = IPL_auction_data.withColumn('BidRank', F.rank().over(window_spec)).filter('BidRank == 1').show()

# COMMAND ----------

window_spec_cumulative = Window().partitionBy('Player').orderBy('Year').rowsBetween(Window.unboundedPreceding, 0)
cumulative_winning_bid = IPL_auction_data.withColumn('CumulativeWinningBid', F.sum('Winning bid').over(window_spec_cumulative))


# COMMAND ----------

cumulative_winning_bid.show(300)

# COMMAND ----------

window_spec_cumulative = Window().partitionBy('Team').orderBy('Year').rowsBetween(Window.unboundedPreceding, 0)
cumulative_winning_bid_by_team = IPL_auction_data.withColumn('CumulativeWinningBid', F.sum('Winning bid').over(window_spec_cumulative))


# COMMAND ----------

cumulative_winning_bid_by_team.show(300)

# COMMAND ----------

amount_spent_by_team=ipl2023auction.groupBy('Team').agg(F.sum("Winning bid").alias("total_amount_spent"))

# COMMAND ----------

amount_spent_by_team.show()

# COMMAND ----------

no_players_by_team=ipl2023auction.groupBy('Team').agg(F.count("Player").alias("No_of_players"))

# COMMAND ----------

no_players_by_team.show()

# COMMAND ----------


