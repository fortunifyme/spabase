#!/usr/bin/env bash

export SPARK_HOME=/opt/spark-2.1.1-bin-hadoop2.7
export PATH=${SPARK_HOME}/bin:$PATH

 current_dir=$(pwd)
echo "curr dir is ==> " ${current_dir}
cd "/home/osboxes/IdeaProjects/hbase_and_co/shc_df"
current_dir=$(pwd)
echo "curr dir is ==> " ${current_dir}
target_dir="target/"
SPARK_APP_PATH="$current_dir/$target_dir"
SPARK_APP_NAME=shc-examples-1.1.2-2.2-s_2.11-SNAPSHOT.jar
SPARK_APP_CLASS_NAME=org.apache.spark.sql.execution.datasources.hbase.CompositeKey
echo ${SPARK_APP_PATH}
echo "curr dir is ==> " ${current_dir}

echo "check that environmental variable configured correctly"

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

else
        echo " === something wrong with your environmental variables please check === "
        echo " === the process will be terminated === "
        echo "no spark"
        exit 1
fi




mvn clean:clean package

echo " ====>> jar ready to run <<==== "


    if [[ -s ${SPARK_APP_PATH}/${SPARK_APP_NAME} ]];then    # file is found and is > 0 bytes.
       ${SPARK_HOME}/bin/spark-submit  \
         --class ${SPARK_APP_CLASS_NAME}  \
        --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11               \
        --repositories http://repo.hortonworks.com/content/groups/public/  \
        --files /opt/hbase-1.1.12/conf/hbase-site.xml                      \
        ${SPARK_APP_PATH}/${SPARK_APP_NAME}
    else                                                    # file is not found or is 0 bytes

       echo '...file ' "${SPARK_APP_NAME}" 'was not found...'
       exit 1
    fi



