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

    dfQuestions = (
        spark.read.option("delimiter", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
            .toDF("Id", "CreationDate", "ClosedDate", "DeletionDate", "Score", "OwnerUserId", "AnswerCount")
    )
    return dfQuestions


# Query Dataframe : Select columns from dfQuestions dataframe
# dfTags.select([c for c in dfTags.columns if c in ['id', 'tag']]).show()
def select_columns_questions(dfQuestions: DataFrame) -> DataFrame:
    return dfQuestions.select("Id", "CreationDate", "ClosedDate", "DeletionDate", "Score", "OwnerUserId",
                              "AnswerCount").show(10)


def run():
    settings = load_settings()
    spark = SparkSession.builder.appName("learn-spark").getOrCreate()

    dfQuestions = load_questions(spark, settings)

    select_columns_questions(dfQuestions)


if __name__ == "__main__":
    run()
