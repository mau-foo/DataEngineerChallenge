import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.lag
import org.apache.spark.sql.functions.unix_timestamp
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.countDistinct

// UTC timezone
val TIME_ZONE = "UTC"   

// Session interval of 15 minutes
val TIME_INTERVAL_THRESHOLD_IN_SECONDS = 15 * 60

// Setting UTC timezone for spark session which is necessary to calculate the difference between timestamp datatypes
spark.conf.set("spark.sql.session.timeZone", TIME_ZONE)

// Read in file in dataframe
var wholeDF = spark.read.text("data/2015_07_22_mktplace_shop_web_log_sample.log.gz")

/* Creating new dataframe to separate out columns from the original dataframe which had all data stored under one column
   Used regex pattern within the split because we are spliting on whitespace and request has multiple words within its column we want to ignore those whitespaces within double quotes
   Regex pattern was found at the following link https://stackabuse.com/regex-splitting-by-character-unless-in-quotes
*/
var splitDF = wholeDF.withColumn("_columns", split($"value", " (?=([^\"]*\"[^\"]*\")*[^\"]*$)")).select(     
    $"_columns".getItem(0).cast(DataTypes.TimestampType).as("time"),
    $"_columns".getItem(1).as("name_of_balancer"),
    $"_columns".getItem(2).as("ip_address"),
    $"_columns".getItem(3).as("private_address"),
    $"_columns".getItem(4).as("request_processing_time"),
    $"_columns".getItem(5).as("backend_processing_time"),
    $"_columns".getItem(6).as("response_processing_time"),
    $"_columns".getItem(7).as("elb_status_code"),
    $"_columns".getItem(8).as("backend_status_code"),
    $"_columns".getItem(9).as("received_bytes"),
    $"_columns".getItem(10).as("sent_bytes"),
    $"_columns".getItem(11).as("request"),
    $"_columns".getItem(12).as("user_agent"),
    $"_columns".getItem(13).as("ssl_cipher"),
    $"_columns".getItem(14).as("ssl_protocol")
)

// First five rows of dataframe
splitDF.show(5)

// Add column - previous timestamp for each record
splitDF = splitDF.withColumn("previous_time", 
                             lag($"time", 1, null).over(Window.partitionBy("ip_address").orderBy("time"))
                            )

// Add column - time difference in seconds between current timestamp and previous timestamp
splitDF = splitDF.withColumn("time_difference", 
                             col("time").cast("long") - col("previous_time").cast("long")
                            )

// Add column - whether the record is new session or not determined by whether time difference it is greater than the 15 minute interval or if time difference is null  
splitDF = splitDF.withColumn("is_new_session",
                             when(col("time_difference").$greater(TIME_INTERVAL_THRESHOLD_IN_SECONDS)
                                  .or(col("time_difference").isNull), 1).otherwise(0)
                            )

// Add column - used to flag each new session per IP address 
splitDF = splitDF.withColumn("session_seq",
                             sum(col("is_new_session")).over(Window.partitionBy(col("ip_address")).orderBy("time"))
                            )

var countByIP = splitDF.groupBy("ip_address").count()

// First five rows of dataframe
countByIP.show(5, false)

var avgSessionTime = splitDF.select(sum("time_difference") / sum("is_new_session")).withColumnRenamed("(sum(time_difference) / sum(is_new_session))", "average_session_time")

// Time is in seconds (conversion in minutes: 25)
avgSessionTime.show()

// Calculate counts of distinct URLs by ip address and session sequence
var countUniqueURL = splitDF.groupBy("ip_address", "session_seq").agg(countDistinct("request").alias("unique_url_count"));

// First five rows of dataframe
countUniqueURL.show(5, false)

// Aggregating to determine the longest session time by session and ip address in seconds
var mostEngagedUsers = splitDF.groupBy("ip_address", "session_seq").sum("time_difference").withColumnRenamed("sum(time_difference)", "total_session_time")

// Display the TOP 10 longest sessions
mostEngagedUsers.sort(desc("sum(time_difference)")).show(10)
