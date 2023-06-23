import os
import configparser
import logging
import smtplib
from email.message import EmailMessage
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, from_json, udf
from pyspark.sql.types import StringType, StructType, StructField
from textblob import TextBlob
import preprocessor as p

logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TwitterSentimentAnalysis:
    """
    A class to perform Twitter sentiment analysis.
    """
    def __init__(self):
        """
        Constructs all the necessary attributes for the TwitterSentimentAnalysis object.

        Reads configuration from 'config.ini' file and creates Spark session. 
        Defines the schema for incoming data.
        """
        self.config = self.read_config('config.ini')
        self.spark = self.create_spark_session()
        self.mySchema = StructType([StructField("text", StringType(), True)])

    def read_config(self, file_path):
        """
        Reads configuration from the given file.

        Parameters:
            file_path (str): The path to the configuration file.

        Returns:
            config: A ConfigParser object with the read configuration.
        """
        config = configparser.ConfigParser()
        config.read(file_path)
        return config

    def create_spark_session(self):
        """
        Creates a SparkSession.

        Returns:
            spark: A SparkSession object.
        """
        try:
            spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
        except Exception as e:
            self.handle_error(f"Failed to create Spark session: {e}")
        return spark

    def send_alert_email(self, error_message):
        """
        Sends an alert email with the given error message.

        Parameters:
            error_message (str): The error message to send.
        """
        email = os.environ.get('ALERT_EMAIL')
        password = os.environ.get('ALERT_EMAIL_PASSWORD')
        smtp_server = os.environ.get('SMTP_SERVER')
        smtp_port = os.environ.get('SMTP_PORT')

        try:
            msg = EmailMessage()
            msg.set_content(f"Alert! An error occurred:\n{error_message}")
            msg['Subject'] = 'Twitter Sentiment Analysis Alert'
            msg['From'] = email
            msg['To'] = email

            server = smtplib.SMTP_SSL(smtp_server, smtp_port)
            server.login(email, password)
            server.send_message(msg)
            server.quit()
        except Exception as e:
            logging.error(f"Failed to send alert email: {e}")

    def handle_error(self, error_message):
        """
        Handles the given error by logging it and sending an alert email.

        Parameters:
            error_message (str): The error message to handle.
        """
        logging.error(error_message)
        self.send_alert_email(error_message)
        exit(1)

    def clean_text(self, text):
        """
        Cleans the given text by removing URLs, mentions, hashtags and reserved words.

        Parameters:
            text (str): The text to clean.

        Returns:
            str: The cleaned text.
        """
        return p.clean(text)

    def preprocessing(self, df):
        """
        Preprocesses the given DataFrame by cleaning and splitting the text into words.

        Parameters:
            df (DataFrame): The DataFrame to preprocess.

        Returns:
            DataFrame: The preprocessed DataFrame.
        """
        clean_text_udf = udf(self.clean_text, StringType())
        words = df.select(explode(split(clean_text_udf(df.text), " ")).alias("word"))
        words = words.na.replace('', None)
        words = words.na.drop()
        return words

    def polarity_detection(self, text):
        """
        Detects the polarity of the given text using TextBlob.

        Parameters:
            text (str): The text to analyze.

        Returns:
            float: The polarity of the text.
        """
        return TextBlob(text).sentiment.polarity

    def subjectivity_detection(self, text):
        """
        Detects the subjectivity of the given text using TextBlob.

        Parameters:
            text (str): The text to analyze.

        Returns:
            float: The subjectivity of the text.
        """
        return TextBlob(text).sentiment.subjectivity

    def text_classification(self, words):
        """
        Classifies the given DataFrame of words based on their polarity and subjectivity.

        Parameters:
            words (DataFrame): The DataFrame to classify.

        Returns:
            DataFrame: The classified DataFrame.
        """
        polarity_detection_udf = udf(self.polarity_detection, StringType())
        subjectivity_detection_udf = udf(self.subjectivity_detection, StringType())
        words = words.withColumn("polarity", polarity_detection_udf("word"))
        words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
        return words

    def handle_schema_evolution(self, current_df, expected_schema):
        """
        Handles schema evolution by matching the current DataFrame's schema to the expected schema.

        Parameters:
            current_df (DataFrame): The current DataFrame.
            expected_schema (StructType): The expected schema.

        Returns:
            DataFrame: The DataFrame with the matched schema.
        """
        current_columns = set(current_df.columns)
        expected_columns = set([field.name for field in expected_schema.fields])
        common_columns = current_columns.intersection(expected_columns)
        return current_df.select([col(column) for column in common_columns])

    def run(self):
        """
        Runs the Twitter sentiment analysis application.
        """
        kafka_servers = self.config['kafka']['servers']
        kafka_topic = self.config['kafka']['topic']
        postgres_url = self.config['postgres']['url']
        postgres_properties = {
            "user": self.config['postgres']['user'],
            "password": os.environ.get('POSTGRES_PASSWORD'),
            "driver": "org.postgresql.Driver"
        }

        try:
            raw_tweets = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", kafka_servers) \
                .option("subscribe", kafka_topic) \
                .load()
        except Exception as e:
            self.handle_error(f"Error connecting to Kafka: {e}")

        try:
            tweet_values = raw_tweets.select(from_json(col("value").cast("string"), self.mySchema).alias("parsed"))
            tweets = tweet_values.select("parsed.*")
            tweets = self.handle_schema_evolution(tweets, self.mySchema)
        except Exception as e:
            self.handle_error(f"Error parsing Kafka stream: {e}")

        try:
            words = self.preprocessing(tweets)
            words = self.text_classification(words)
        except Exception as e:
            self.handle_error(f"Error processing or classifying text: {e}")

        try:
            words.writeStream \
                .foreachBatch(lambda df, epoch_id: df.write.jdbc(
                    url=postgres_url,
                    table="sentiment_analysis",
                    mode="append",
                    properties=postgres_properties)) \
                .start() \
                .awaitTermination()
        except Exception as e:
            self.handle_error(f"Error writing to PostgreSQL database: {e}")

if __name__ == "__main__":
    TwitterSentimentAnalysis().run()
