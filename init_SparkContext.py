def init_SparkContext(appName):
    """This function is used to initialize spark context and Spark Session
       Argument: AppName
       Return: Spark Context and Spark Session
    """
    import findspark
    findspark.init()
    print("Spark found in your system !!")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local").appName(appName).getOrCreate()
    sc = spark.sparkContext
    print("Spark Context and Spark session initialized !!")
    return sc, spark
