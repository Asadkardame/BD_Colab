#!/bin/bash

# =================================================================
# When executing the scala-spark script, use the following packages and JARs :
# -----------------------------------------------------------------

spark-shell \
--jars /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hbase/lib/hbase-client-2.2.3.7.1.7.0-551.jar,\
/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hbase/lib/hbase-common-2.2.3.7.1.7.0-551.jar,\
/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/lib/hbase/lib/hbase-protocol-2.2.3.7.1.7.0-551.jar,\
/opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/hbase-shaded-client-2.2.3.7.1.7.0-551.jar \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4

#-----------------------------------------------------------------

