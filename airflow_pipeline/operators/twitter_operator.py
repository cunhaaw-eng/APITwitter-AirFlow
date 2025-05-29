import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path
import os
import re
from collections import Counter
import csv

class TwitterOperator(BaseOperator):
    template_fields = ["query", "file_path", "start_time", "end_time"]

    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time
        self.start_time = start_time
        self.query = query
        self.file_path = file_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        Path(self.file_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()

        with open(self.file_path, "w") as output_file:
            for pg in TwitterHook(self.end_time, self.start_time, self.query).run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

class TweetCleanerOperator(BaseOperator):
    template_fields = ["input_path", "output_path"]

    def __init__(self, input_path, output_path, **kwargs):
        self.input_path = input_path
        self.output_path = output_path
        super().__init__(**kwargs)

    def clean_text(self, text):
        text = re.sub(r"http\S+", "", text)
        text = re.sub(r"[^a-zA-ZÀ-ÿ0-9\s]", "", text)
        return text.lower().strip()

    def create_parent_folder(self):
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()

        cleaned = []
        with open(self.input_path, "r") as f:
            for line in f:
                tweet = json.loads(line)
                if "data" in tweet:
                    for t in tweet["data"]:
                        text = self.clean_text(t["text"])
                        cleaned.append({"text": text})

        with open(self.output_path, "w") as out:
            json.dump(cleaned, out, ensure_ascii=False, indent=2)

class WordCountOperator(BaseOperator):
    template_fields = ["input_path", "output_path"]

    def __init__(self, input_path, output_path, **kwargs):
        self.input_path = input_path
        self.output_path = output_path
        super().__init__(**kwargs)

    def create_parent_folder(self):
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        self.create_parent_folder()

        with open(self.input_path, "r") as f:
            tweets = json.load(f)

        counter = Counter()
        for tweet in tweets:
            words = tweet["text"].split()
            counter.update(words)

        with open(self.output_path, "w", newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["word", "count"])
            for word, count in counter.most_common(50):
                writer.writerow([word, count])


