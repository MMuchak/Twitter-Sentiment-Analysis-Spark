import unittest
from unittest.mock import patch, MagicMock
from app.main import TwitterSentimentAnalysis

class TestTwitterSentimentAnalysis(unittest.TestCase):
    
    def setUp(self):
        # Create an instance of TwitterSentimentAnalysis for testing
        self.tsa = TwitterSentimentAnalysis()

    def test_clean_text(self):
        # Define a test case
        test_text = "Hello #world http://test.com"
        expected_output = "Hello world"
        # Run the method with the test case
        output = self.tsa.clean_text(test_text)
        # Assert that the output is as expected
        self.assertEqual(output, expected_output)

    @patch('your_script.TwitterSentimentAnalysis.get_kafka_stream')
    def test_run(self, mock_get_kafka_stream):
        # Create a mock Spark DataFrame
        mock_df = MagicMock()
        # Make the mock method return the mock DataFrame
        mock_get_kafka_stream.return_value = mock_df
        # Run the method
        self.tsa.run()
        # Assert that the mock DataFrame was used as expected
        mock_df.select.assert_called_once()
        mock_df.writeStream.foreachBatch.assert_called_once()

# This allows the test to be run from the command line
if __name__ == '__main__':
    unittest.main()
