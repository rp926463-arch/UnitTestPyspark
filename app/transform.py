import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from app.conftest import SparkInstanceBuilder


class Transform(SparkInstanceBuilder):
    def __init__(self):
        super().__init__(environment="test", app_name="test_app")
        self.spark = self.buildSpark()

    def sample_transform(self, input_df: DataFrame) -> DataFrame:
        inter_df = input_df.where(input_df['that_column'] == \
                                  F.lit('hobbit')).groupBy('another_column').agg(
            F.sum('yet_another').alias('new_column'))
        output_df = inter_df.select('another_column', 'new_column',
                                    F.when(F.col('new_column') > 10, 'yes').otherwise('no').alias('indicator')).where(
            F.col('indicator') == F.lit('yes'))
        return output_df
