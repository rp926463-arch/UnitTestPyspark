import unittest
from unittest.mock import patch, Mock, mock_open
from app.conftest import SparkInstanceBuilder


class TestSparkInstanceBuilder(unittest.TestCase):
    @patch('app.conftest.SparkSession')
    def test_build_spark_success(self, mock_spark_session):
        # Mocking external dependencies
        mock_spark_session.builder.config.return_value.getOrCreate.return_value.newSession.return_value = Mock()
        with patch('builtins.open', mock_open(read_data='{"sparkSessionProperties": {"spark.executor.cores": "2", '
                                                        '"spark.cores.max": "4", "spark.driver.memory": "2g", '
                                                        '"spark.executor.memory": "2g"}, "appConfig": {'
                                                        '"EVENT_LOG_DIRECTORY": "/path/to/event/logs"}}')) as mock_file:
            # Setting up test data
            spark_builder = SparkInstanceBuilder(environment="test", app_name="test_app")

            # Calling the method under test
            spark_instance = spark_builder.buildSpark(env="test")

            # Assertions
            self.assertIsInstance(spark_instance, Mock)
            mock_spark_session.builder.config.assert_called_once()
            mock_spark_session.builder.config.return_value.getOrCreate.assert_called_once()
            mock_spark_session.builder.config.return_value.getOrCreate.return_value.newSession.assert_called_once()

    @patch('app.conftest.SparkSession')
    def test_build_spark_failure(self, mock_spark_session):
        with patch('builtins.open', mock_open(read_data='{}')) as mock_file:
            # Setting up test data
            spark_builder = SparkInstanceBuilder(environment="test", app_name="test_app")

            # Calling the method under test
            result = spark_builder.buildSpark(env="test")

            # Assertions
            self.assertEqual(result, "Failure")
            mock_spark_session.builder.config.assert_not_called()
            mock_spark_session.builder.config.return_value.getOrCreate.assert_not_called()
            mock_spark_session.builder.config.return_value.getOrCreate.return_value.newSession.assert_not_called()


if __name__ == "__main__":
    unittest.main()
