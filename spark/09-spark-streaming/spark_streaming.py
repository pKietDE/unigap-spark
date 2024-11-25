from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext


spark = SparkSession.builder.appName("spark-streaming").master("spark://spark:7077").getOrCreate()
# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = spark.sparkContext
ssc = StreamingContext(sc,20)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("172.18.0.11", 9999)

# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# In ra danh sách các từ có số lần xuất hiện là chẵn
evenCounts = wordCounts.filter(lambda x: x[1] % 2 == 0)
evenCounts.pprint()

# In ra danh sách các từ có độ dài lớn hơn 1 và số lần xuất hiện là lẻ
oddLengthWords = wordCounts.filter(lambda x: len(x[0]) > 1 and x[1] % 2 != 0)
oddLengthWords.pprint()

ssc.start()  # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate