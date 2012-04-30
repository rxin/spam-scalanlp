#!/usr/bin/env bash

# Set Spark environment variables for your site in this file. Some useful
# variables to set are:
# - MESOS_HOME, to point to your Mesos installation
# - SCALA_HOME, to point to your Scala installation
# - SPARK_CLASSPATH, to add elements to Spark's classpath
# - SPARK_JAVA_OPTS, to add JVM options
# - SPARK_MEM, to change the amount of memory used per node (this should
#   be in the same format as the JVM's -Xmx option, e.g. 300m or 1g).
# - SPARK_LIBRARY_PATH, to add extra search paths for native libraries.

export SPARK_MEM=2g

SPARK_JAVA_OPTS=" -Dspark.cache.class=spark.SerializingCache"
SPARK_JAVA_OPTS+=" -Dspark.serializer=spark.KryoSerializer"
SPARK_JAVA_OPTS+=" -Dspark.kryo.registrator=wiki.KryoRegistrator"
SPARK_JAVA_OPTS+=" -Dspark.kryoserializer.buffer.mb=128"
SPARK_JAVA_OPTS+=" -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCTimeStamps -XX:+UseParallelGC"

export SPARK_JAVA_OPTS

export SPARK_CLASSPATH="$HOME/.ivy2/cache/org.apache.lucene/lucene-analyzers/jars/lucene-analyzers-3.5.0.jar:$HOME/.ivy2/cache/org.apache.lucene/lucene-core/jars/lucene-core-3.5.0.jar"

