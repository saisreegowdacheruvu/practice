from pyspark.sql import SparkSession
from distutils import strtobool
import os
import yaml
if __name__ == "__main__":
    #creating spark session
    spark = SparkSession \
         .builder \
         .appName('RDD Examples') \
         .master("local[*]") \
         .getOrCreate()
    spark.SparkContext.setLogLevel("error")

    current_dir=os.path.abspath(os.path.dirname(__file__))
    conf_app_path=os.path.abspath(current_dir +"/../" +application.yml)
    conf_secret_path=os.path.abspath(current_dir+"/../" +.secrets)

    conf=open(conf_app_path)
    app_path=yaml.load(conf,Loader=yaml.FullLoader)
    conf1=open(conf_secret_path)
    secret_path=yaml.load(conf1,Loader=yaml.FullLoader)

    #setting up access to s3
    hadoop_conf=spark.SparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fa.s3a.access.key",secret_path["s3_conf"]["access_key"])
    hadoop_conf.set("fa.s3a.secret.key",secret_path["s3_conf"]["secret_access_key"])

    demographics_rdd = spark.SparkContext.textFile("s3a://" +app_path['s3_conf']['s3_bucket']+ "/demographics.csv")
    finances_rdd = spark.SparkContext.textFile("s3a://" +app_path['s3_conf']['s3_bucket'] + "/finances.csv")

    demographics_pair_rdd = demographics_rdd \
        .map(lambda a: a.split(',')) \
        .map(lambda b: ((int(b[0])), ((b[1]), strtobool(b[2]), (b[3]), (b[4]), strtobool(b[5]), strtobool(b[6]), int(b[7])))) \
        .filter(lambda c: c[1][2] == "Switzerland")
    finances_pair_rdd = finances_rdd \
        .map(lambda a: a.split(',')) \
        .map(lambda b: ((int(b[0])), (strtobool(b[1]), strtobool(b[2]), strtobool(b[3]), b[4]))) \
        .filter(lambda c:(c[1][0]==1  and c[1][1]==1))

    result_rdd=demographics_pair_rdd.join(finances_pair_rdd)

    result_rdd.foreach(print)

