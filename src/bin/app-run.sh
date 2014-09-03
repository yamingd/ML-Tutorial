#!/usr/bin/env bash
APP_NAME=$1
shift

SPARK_HOME="/usr/local/spark/spark-0.8.1-incubating-bin-hadoop2"
APP_FOLDER="/home/hduser/apps/$APP_NAME"

# Export this as SPARK_HOME
export SPARK_HOME="$SPARK_HOME"

# Load environment variables from conf/spark-env.sh, if it exists
if [ -e "$SPARK_HOME/conf/spark-env.sh" ] ; then
  . $SPARK_HOME/conf/spark-env.sh
fi


export SPARK_APP_JAR=`ls "$APP_FOLDER"/*.jar`

if [[ -z SPARK_APP_JAR ]]; then
  echo "Failed to find Spark App assembly in $APP_FOLDER" >&2
  exit 1
fi

echo "APP FOLDER:", $APP_FOLDER
echo "SPARK_HOME:", $SPARK_HOME
echo "SPARK_APP_JAR:", $SPARK_APP_JAR

# Since the examples JAR ideally shouldn't include spark-core (that dependency should be
# "provided"), also add our standard Spark classpath, built using compute-classpath.sh.
CLASSPATH=`$SPARK_HOME/bin/compute-classpath.sh`
CLASSPATH="$SPARK_APP_JAR:$CLASSPATH"

echo $CLASSPATH

# Find java binary
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Set JAVA_OPTS to be able to load native libraries and to set heap size
JAVA_OPTS="$SPARK_JAVA_OPTS"
#JAVA_OPTS="$JAVA_OPTS -Djava.library.path=$SPARK_LIBRARY_PATH"
# Load extra JAVA_OPTS from conf/java-opts, if it exists
if [ -e "$SPARK_HOME/conf/java-opts" ] ; then
  JAVA_OPTS="$JAVA_OPTS `cat $SPARK_HOME/conf/java-opts`"
fi
export JAVA_OPTS

echo -n "Spark Command: "
echo -n "$@"
echo
echo "$RUNNER" -cp "$CLASSPATH" $JAVA_OPTS "$APP_ARGS"
echo "========================================"
echo

exec "$RUNNER" -cp "$CLASSPATH" $JAVA_OPTS "$@"

