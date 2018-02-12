#!/usr/bin/env bash

export SPARK_HOME=/opt/spark-2.1.1-bin-hadoop2.7
export PATH=${SPARK_HOME}/bin:$PATH
current_dir=$(pwd)
target_dir="target/scala-2.11"
APP_PATH="$current_dir/$target_dir"
APP_NAME=rt-spark-engine-assembly-1.0.jar
SPARK_APP_CLASS_NAME=Main
echo ${APP_PATH}
echo " === current directory is  === "
echo ${current_dir}

echo " === check that environmental variable configured correctly === "

# ${SPARK_HOME}/bin/spark-submit —version
# Changed code to remove the 'head -1' as per the suggestion in comment.
# JAVA_VERSION=java -version 2>&1 #|awk 'NR==1{ gsub(/"/,""); print $3 }'
# export JAVA_VERSION
# echo $JAVA_VERSION


#string =  ${SPARK_HOME}
   # if [[  ${SPARK_HOME} = *[!\ ]* ]]; then
   #if [[ -d ${SPARK_HOME} ]]; then
if type -p java; then
    echo found java executable in PATH
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    echo found java executable in JAVA_HOME
    _java="$JAVA_HOME/bin/java"
else
    echo "no java"
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.7" ]]; then
        echo version is more than 1.7
    else
        echo version is less than 1.7
    fi
fi


################


if [[ -n ${SPARK_HOME} ]] && [[ -x "$SPARK_HOME/bin/spark-submit" ]];  then
    echo found spark executable in ${SPARK_HOME}
    #line below : check the version of the spark
   # ${SPARK_HOME}/bin/spark-submit —version
else
        echo " === something wrong with your environmental variables please check === "
        echo " === the process will be terminated === "
        echo "no spark"
        exit 1
fi


#sbt clean assembly

# sbt clean
# sbt package

#echo " ==== jar ready to run ==== "
cd ${target_dir}
echo " ==== check curr dir  ==== "
echo $(pwd)
#while read file_a <&3; do
    #if condition not working properly -> always true => something wrong with DIR_A and file_a ( not correct registration )
    if [[ -s ${APP_PATH}/${APP_NAME} ]];then    # file is found and is > 0 bytes.
      #  --master local[*] \
       ${SPARK_HOME}/bin/spark-submit  \
        --conf spark.hbase.host=192.168.56.106 \
        --master "local[*]" \
        --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1 \
        --packages it.nerdammer.bigdata:spark-hbase-connector_2.10:1.0.3  \
        --exclude-packages org.slf4j:slf4j-api \
        --class Main rt-spark-engine-assembly-1.0.jar \
         raw-gpstrajectory-data-topic result-aggregation-topic localhost localhost:2181

    else                          # file is not found or is 0 bytes

       echo '...file ' "${APP_NAME}" 'was not found...'
       exit 1
    fi
#done



#--conf spark.hbase.host=192.168.56.106 \
#            --packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.1.1, it.nerdammer.bigdata:spark-hbase-connector_2.10:1.0.3 \
 #           --exclude-packages org.slf4j:slf4j-api  \
  #          --class ${SPARK_APP_CLASS_NAME}         \
   #             ${SPARK_APP_NAME}    \
    #        --raw-gpstrajectory-data-topic          \
     #       --result-aggregation-topic              \
      #      --localhost                             \
       #     --localhost:2181                        \


    # the script below check the version of scala => need review and finalize
 # if [[  $(${SPARK_HOME}/bin/spark-submit —version 2>&1 | grep *"Scala"*) ]]; then
 #        echo ${SPARK_HOME}
 #        echo " === environmental variable configured correctly === "
#
    #else
    #    echo " === something wrong with your environmental variables please check === "
    #    echo " === the process will be terminated === "
    #    exit 1
#fi