import sys 
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from airflow.utils.dates import days_ago
from os.path import join
from datetime import datetime
from operators.twitter_operator import TwitterOperator, TweetCleanerOperator, WordCountOperator

# DAG 1: Extração Diária dos tweets

with DAG(dag_id="TwitterDAG",
         start_date=days_ago(2),
         schedule_interval="@daily") as dag_extract:

    query = "datascience"

    to = TwitterOperator(
        file_path=join("datalake/twitter_datascience",
                       "extract_date={{ ds }}",
                       "datascience_{{ ds_nodash }}.json"),
        query=query,
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",
        task_id="twitter_datascience"
    )


# DAG 2: Processamento de Tweets

with DAG(dag_id="TwitterProcessDAG",
         start_date=days_ago(2),
         schedule_interval="@daily") as dag_process:

    cleaner = TweetCleanerOperator(
        input_path=join("datalake/twitter_datascience",
                        "extract_date={{ ds }}",
                        "datascience_{{ ds_nodash }}.json"),
        output_path=join("datalake/processed_tweets",
                         "extract_date={{ ds }}",
                         "cleaned_tweets.json"),
        task_id="clean_transform_tweets"
    )

# DAG 3: Contagem de palavras mais frequntes

with DAG(dag_id="WordCountDAG",
         start_date=days_ago(2),
         schedule_interval="@daily") as dag_wordcount:

    counter = WordCountOperator(
        input_path=join("datalake/processed_tweets",
                        "extract_date={{ ds }}",
                        "cleaned_tweets.json"),
        output_path=join("datalake/word_count",
                         "extract_date={{ ds }}",
                         "word_count.csv"),
        task_id="generate_word_count"
    )
