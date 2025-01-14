# The cibuildwheels manylinux image runs CentOS
yum install java-11 -y
yum install wget -y

mkdir spark_installation
cd spark_installation
wget https://blobs.duckdb.org/ci/spark-3.5.3-bin-hadoop3.tgz
tar -xvzf spark-3.5.3-bin-hadoop3.tgz
mv spark-3.5.3-bin-hadoop3 spark
