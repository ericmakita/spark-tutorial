import logging

from pyspark.sql import SparkSession, DataFrame

from ingest.conf.settings import Settings, load_settings


def load_questions(spark: SparkSession, settings: Settings) -> DataFrame:
    """Read question dataset from input data

    :param spark: sparkSession
    :param settings: settings of paths
    :return:
    """
    path = settings.input_files_path + "/questions/*.csv"
    logging.info("load raw questions from " + path)

    raw_questions = (
        spark.read.option("delimiter", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
    )
    return raw_questions


def load_tags(spark: SparkSession, settings: Settings) -> DataFrame:
    """Read question dataset from input data

    :param spark: sparkSession
    :param settings: settings of paths
    :return:
    """
    path = settings.input_files_path + "/tags/*.csv"
    logging.info("load raw questions from " + path)

    raw_tags = (
        spark.read.option("delimiter", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
    )
    return raw_tags


def run():
    settings = load_settings()
    spark = SparkSession.builder.appName("questions").getOrCreate()

    raw_tags = load_tags(spark, settings)

    raw_tags.printSchema()


if __name__ == "__main__":
    run()
