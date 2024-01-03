import unittest
from unittest.mock import patch
from pyspark.sql import SparkSession
from app.transform import Transform


class TestTransform(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    def tearDown(self):
        pass

    def test_sample_transform(self):
        # Create a test DataFrame
        schema = ['that_column', 'another_column', 'yet_another']
        data = [
            ('hobbit', 'Samwise', 5),
            ('hobbit', 'Billbo', 50),
            ('hobbit', 'Billbo', 20),
            ('wizard', 'Gandalf', 1000)
        ]
        test_df = self.spark.createDataFrame(data, schema=schema)
        test_df.printSchema()

        # Mock the Transform class to prevent calling super().__init__()
        with patch.object(Transform, '__init__', lambda x: None):
            # Create an instance of the Transform class without calling super()
            transformer = Transform()

        # Call the sample_transform method
        result_df = transformer.sample_transform(test_df)
        result_df.show()

        # Perform assertions on the result DataFrame
        # self.assertEqual(result_df.count(), 1)
        # self.assertEqual(result_df.first()['another_column'], 'Billbo')
        # self.assertEqual(result_df.first()['new_column'], 70)
        # self.assertEqual(result_df.first()['indicator'], 'yes')


if __name__ == "__main__":
    unittest.main()
