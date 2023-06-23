# Twitter Sentiment Analysis with PySpark

This project is a real-time sentiment analysis application. It processes and classifies tweets using PySpark and TextBlob, reads them from a Kafka topic, and stores the analyzed results in a PostgreSQL database.

## Table of Contents

- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [How it Works](#how-it-works)
- [License](#license)
- [Contributing](#contributing)

## Project Structure

Twitter-Sentiment-Analysis-Spark/
|--- app/
| |--- init.py
| |--- main.py
|--- tests/
| |--- init.py
| |--- test.py
|--- config.ini
|--- requirements.txt
|--- README.md
|--- LICENSE


- The `app/` directory contains the main application code.
- The `tests/` directory contains the unit tests for the application.
- `config.ini` is the configuration file for Kafka and PostgreSQL settings.
- `requirements.txt` lists the required packages and their versions.
- `README.md` (this file) provides information about the project.
- `LICENSE` contains the project's license information.

## Getting Started

To get started with the project, follow these steps:

1. Install the required packages listed in `requirements.txt`:
```bash
pip install -r requirements.txt
```
2. Create a PostgreSQL table using the following command:

```bash
CREATE TABLE sentiment_analysis (
word TEXT NOT NULL,
polarity TEXT NOT NULL,
subjectivity TEXT NOT NULL
);
```


3. Set up the following environment variables for email alert functionality:

- `ALERT_EMAIL`: the email address to send alerts to
- `ALERT_EMAIL_PASSWORD`: the password for the alert email account
- `SMTP_SERVER`: the SMTP server to use for sending emails
- `SMTP_PORT`: the port to use for the SMTP server
- `POSTGRES_PASSWORD`: the password for your PostgreSQL database

4. Configure the Kafka and Postgres settings in `config.ini`. See Configuration for more details.

5. Run the main script:

```bash
python app/main.py
```

6. To run tests, execute:
```bash
python -m unittest tests/test.py
```

## Configuration

You need to provide configuration for Kafka and Postgres in `config.ini` as follows:

```bash[kafka]
servers = localhost:9092
topic = twitter

[postgres]
url = jdbc:postgresql://localhost:5432/twitter
user = postgres
```

Please replace `localhost:9092`, `twitter`, and `localhost:5432` with your actual Kafka servers and topic, and PostgreSQL URL respectively.

## How it Works

The application reads a stream of tweets from a Kafka topic. The tweets are then preprocessed and each word is classified based on its sentiment polarity and subjectivity using TextBlob. The processed data is then stored in a PostgreSQL database.

## License

This project is licensed under the terms of the MIT license. See the `LICENSE` file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
