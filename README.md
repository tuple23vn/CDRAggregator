# CDRAggregator

To build the app, go to the app's directory and type 

sbt package

A jar file is created for you. To run the app, run the following command

{YOUR_SPARK_HOME}/bin/spark-submit \
  --class "SimpleApp" \
  --master local[4] \
target/scala-2.11/simpleapp_2.11-0.1.jar


where {YOUR_SPARK_HOME} is the path where spark is intalled in your machine

Normally it is (OSX) /usr/local/Cellar/apache-spark/2.3.1/libexec
