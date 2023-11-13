import urllib.request
import tarfile
import tempfile
import os

dirname = os.path.dirname(os.path.realpath(__file__))
tf = tempfile.NamedTemporaryFile()

urllib.request.urlretrieve(
    'https://github.com/duckdb/duckdb-data/releases/download/v1.0/ldbc-snb-sf0.1.tar.gz', tf.name
)
tarfile.open(tf.name).extractall(dirname)
