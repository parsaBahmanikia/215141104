"""
    This Spark app connects to the data source script running in another Docker container on port 9999, which provides a stream of random integers.
    The data stream is read and processed by this spark app, where multiples of 9 are identified, and statistics are sent to a dashboard for visualization.
    Both apps are designed to be run in Docker containers.

    Made for: EECS 4415 - Big Data Systems (Department of Electrical Engineering and Computer Science, York University)
    Author: Changyuan Lin
"""
import re
import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
import json
import statistics
def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']
def extract_language_name(ls_dict):
    for elem in ls_dict:
        for dic in elem:
            yield (dic["language"],(1,dic["stargazers_count"]))
def extract_description(ls_dict):
    for elem in ls_dict:
        for dic in elem:
            if dic["description"] is not None:
                description=re.sub("[^a-zA-Z ]", "", dic["description"])
                ds_list=re.split(" ", description)
                for word in ds_list:
                    if word != " " or word!="":

                        yield ((dic["language"],word),1)
def send_df_count_avgstars_to_dashboard(df):
    url = 'http://webapp:5000/updateData'
    data1= df.toPandas().to_dict('list')
    requests.post(url, json=data1)
def process_rdd_counts_avg(time,rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(language=w[0], count=w[1][0], average_stars=w[1][1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("result1")
        new_results_df = sql_context.sql("select language, count, average_stars from result1")
        new_results_df.show()
        send_df_count_avgstars_to_dashboard(new_results_df)

    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)


def process_rdd_top10_words(time,rdd):
    pass
    print("----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        row_rdd = rdd.map(lambda w: Row(language=w[0], top10words=w[1]))
        results_df = sql_context.createDataFrame(row_rdd)
        results_df.createOrReplaceTempView("result2")
        new_results_df = sql_context.sql("select language, top10words from result2")
        new_results_df.show(truncate=False)

    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)



def aggr_function(new_values, last_state):
    countCounter=0
    avgS=0
    counts=[elem[0] for elem in new_values]
    stars=[elem[1] for elem in new_values]
    if last_state is not None:
        countCounter=last_state[0]
        avgS=last_state[1]
    else:
        last_state=[0,0]
    res= (sum(counts) + countCounter, ((avgS*countCounter) + sum(stars))/ (countCounter+ sum(counts)))
    return res
def words_aggregate(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def process_rdd(rdd):
    sorted_rdd=rdd.map(lambda t: sorted(t, key=lambda x: x[1]))
    return sorted_rdd
if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="NineMultiples")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("checkpoint_NineMultiples")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    dict_data=data.map(lambda x: json.loads(x))
    dict_ls=dict_data.map(lambda dic: dic["items"])
    repo_count_stars=dict_ls.mapPartitions(lambda dic:extract_language_name(dic)).reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    aggregated_counts = repo_count_stars.updateStateByKey(aggr_function)

    description_info=dict_ls.mapPartitions(lambda dic:extract_description(dic)).reduceByKey(lambda a, b: a + b)
    words=description_info.updateStateByKey(words_aggregate)
    sorted_ = words.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
    words=sorted_.map(lambda data: (data[0][0],(data[0][1], data[1]))).groupByKey().mapValues(list)
    top10_words=words.map(lambda data:(data[0], data[1][:10]))
    aggregated_counts.pprint()
    aggregated_counts.foreachRDD(process_rdd_counts_avg)
    top10_words.pprint()
    top10_words.foreachRDD(process_rdd_top10_words)



    #words=language_inof.map(lambda data: (data[0][0],(data[0][1], data[1])))
    #words.pprint()


    #counts = numbers.map(lambda num: ("Yes" if num % 9 == 0 else "No", 1)).reduceByKey(lambda a, b: a+b)
    #windowedCounts = numbers.map(lambda num: ("Yes" if num % 9 == 0 else "No", 1)).reduceByKeyAndWindow(lambda x, y: x + y, lambda x, y: x - y, 20, 20)
    #windowedCounts.pprint()
    #aggregated_counts = counts.updateStateByKey(aggregate_count)
    #aggregated_counts.foreachRDD(process_rdd)
    ssc.start()
    ssc.awaitTermination()
