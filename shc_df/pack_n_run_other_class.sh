#!/usr/bin/env bash

export SPARK_HOME=/opt/spark-2.1.1-bin-hadoop2.7
export PATH=${SPARK_HOME}/bin:$PATH
 current_dir=$(pwd)
target_dir="target/"
SPARK_APP_PATH="$current_dir/$target_dir"
SPARK_APP_NAME=shc-examples-1.1.2-2.2-s_2.11-SNAPSHOT.jar
SPARK_APP_CLASS_NAME=org.apache.spark.sql.execution.datasources.hbase.CompositeKey
echo ${SPARK_APP_PATH}
echo ${current_dir}

echo "check that environmental variable configured correctly"

# ${SPARK_HOME}/bin/spark-submit --version
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
        echo version is more than 1.5
    else
        echo version is less than 1.5
    fi
fi


################


if [[ -n ${SPARK_HOME} ]] && [[ -x "$SPARK_HOME/bin/spark-submit" ]];  then
    echo found spark executable in ${SPARK_HOME}
    #line below : check the version of the spark
   # ${SPARK_HOME}/bin/spark-submit --version
else
        echo " === something wrong with your environmental variables please check === "
        echo " === the process will be terminated === "
        echo "no spark"
        exit 1
fi




mvn clean:clean package

echo " ==== jar ready to run ==== "

#while read file_a <&3; do
    #if condition not working properly -> always true => something wrong with DIR_A and file_a ( not correct registration )
    if [[ -s ${SPARK_APP_PATH}/${SPARK_APP_NAME} ]];then    # file is found and is > 0 bytes.
       ${SPARK_HOME}/bin/spark-submit  \
         --class ${SPARK_APP_CLASS_NAME}  \
        --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11               \
        --repositories http://repo.hortonworks.com/content/groups/public/  \
        --files /opt/hbase-1.1.12/conf/hbase-site.xml                      \
        ${SPARK_APP_PATH}/${SPARK_APP_NAME}
    else                          # file is not found or is 0 bytes

       echo '...file ' "${SPARK_APP_NAME}" 'was not found...'
       exit 1
    fi
#done






    # the script below check the version of scala => need review and finalize
 # if [[  $(${SPARK_HOME}/bin/spark-submit --version 2>&1 | grep *"Scala"*) ]]; then
 #        echo ${SPARK_HOME}
 #        echo " === environmental variable configured correctly === "
#
    #else
    #    echo " === something wrong with your environmental variables please check === "
    #    echo " === the process will be terminated === "
    #    exit 1
   #fi