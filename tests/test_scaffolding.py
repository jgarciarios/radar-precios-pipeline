import pandas
import pyarrow
import duckdb

import pipeline


def test_pipeline_package_importable():
    assert pipeline is not None


def test_data_libs_importable():
    assert pandas.__version__
    assert pyarrow.__version__
    assert duckdb.__version__
