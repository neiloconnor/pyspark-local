# PySpark Local

Minimal implementation of the PySpark API to make it easier to write and locally test PySpark code. You can run your PySpark code against this local API to verify it works with sample data before using the real PySpark API in production.

## Getting started
Copy `pyspark.py` into your project directory.

## Interactive mode
You can emulate pysparks interactive mode by running the command
```shell
python -i pyspark.py
```

## Script mode
Or import and use in your Python scripts.
```python
from pyspark import SparkContext
```

## Disclaimer
The set of features is incomplete, but commonly used features were chosen based on https://s3.amazonaws.com/assets.datacamp.com/blog_assets/PySpark_Cheat_Sheet_Python.pdf
