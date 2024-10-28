# The cibuildwheels manylinux image runs CentOS
yum install java-11 -y
yum install wget -y

mkdir spark_installation
cd spark_installation
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
tar -xvzf spark-3.5.3-bin-hadoop3.tgz
mv spark-3.5.3-bin-hadoop3 spark