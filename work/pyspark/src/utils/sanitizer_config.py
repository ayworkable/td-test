"""
Sanitizer configs
:col_name: column name of the data frame to be sanitized
:pyspark_rule: sanitizer rule using pyspark sql functions `import pyspark.sql.functions as f` eg. f.lower(f.col(col_name))
"""
class SanitizerConfig:
    def __init__(self, col_name: str, pyspark_rule: str) -> None:
        self.col_name = col_name
        self.pyspark_rule = pyspark_rule
