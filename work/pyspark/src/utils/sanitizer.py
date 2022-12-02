import pyspark.sql.functions as f
from pyspark.sql.dataframe import DataFrame
from typing import List
from utils.sanitizer_config import SanitizerConfig


class Sanitizer:
    def __init__(self, sanitizer_configs: List[SanitizerConfig]) -> None:
        self.sanitizer_configs = sanitizer_configs

    def sanitize_df(self, df: DataFrame) -> DataFrame:
        tmp_df = df
        for config in self.sanitizer_configs:
            tmp_df = tmp_df.withColumn(config.col_name, eval(config.pyspark_rule))
        return tmp_df
