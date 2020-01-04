import logging

from pyspark.sql import SparkSession, DataFrame

from ingest.conf.settings import Settings, load_settings


def load_tags(spark: SparkSession, settings: Settings) -> DataFrame:
    """Read question dataset from input data

    :param spark: sparkSession
    :param settings: settings of paths
    :return:
    """
    path = settings.input_files_path + "/tags/*.csv"
    logging.info("load raw questions from " + path)

    dfTags = (
        spark.read.option("delimiter", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
            .toDF("id", "tag")
    )
    return dfTags


def select_columns_tags(dfTags: DataFrame) -> DataFrame:
    # Query dataframe: Select columns from dataframe

    return dfTags.select("id", "tag").show(10)


# DataFrame Query: filter by column value of a dataframe
def filter_tags(dfTags: DataFrame) -> DataFrame:
    return dfTags.filter(dfTags['tag'] == 'php').show(10)


# DataFrame Query: count rows of a dataframe
def count_rows(dfTags: DataFrame):
    return print("The number of php tags is :",
                 dfTags.filter(dfTags['tag'] == 'php').count())


# DataFrame Query: SQL like query

def sql_like_query(dfTags: DataFrame) -> DataFrame:
    return dfTags.filter("tag like 's%'").show(10)


# DataFrame Query: Multiple filter chaining

def multiple_filter(dfTags: DataFrame) -> DataFrame:
    return dfTags.filter("id == 25 or id == 108").filter("tag like 's%'").show(10)


def run():
    settings = load_settings()
    spark = SparkSession.builder.appName("tags-analysis").getOrCreate()

    dfTags = load_tags(spark, settings)
    filter_tags(dfTags)
    count_rows(dfTags)
    sql_like_query(dfTags)
    multiple_filter(dfTags)


if __name__ == "__main__":
    run()
