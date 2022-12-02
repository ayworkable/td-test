from pyspark.sql import SparkSession
import sys
import json
import os
from typing import List
from utils.argument_handler import ArgumentHandler
from utils.sanitizer import Sanitizer
from utils.sanitizer_config import SanitizerConfig


def get_sanitizer_configs(config_path: str) -> List[SanitizerConfig]:
    sanitizer_configs = []
    with open(config_path) as config_json:
        config_dicts = json.load(config_json)
        for config_dict in config_dicts:
            sanitizer_config = SanitizerConfig(
                col_name=config_dict["col_name"],
                pyspark_rule=config_dict["pyspark_rule"],
            )
            sanitizer_configs.append(sanitizer_config)
    return sanitizer_configs


if __name__ == "__main__":
    app_name = os.path.basename(__file__)
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    # validate argv
    expected_argv = ["input_path", "output_path", "config_path"]
    argument_handler = ArgumentHandler(argv=expected_argv)
    argument_handler.validate_required_config_argv()
    argv = argument_handler.get_sys_args()

    # extract source
    source_df = spark.read.format("text").option("wholetext", True).load(argv[0])

    # set sanitize config
    sanitizer_configs = get_sanitizer_configs(config_path=argv[2])

    # sanitize
    sanitizer = Sanitizer(sanitizer_configs=sanitizer_configs)
    sanitized_df = sanitizer.sanitize_df(df=source_df)
    sanitized_df.show()

    # write as df
    sanitized_df.write.format("text").save(argv[1])

    spark.stop()
