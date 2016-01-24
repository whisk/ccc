#!/bin/bash

NAME="Task11"

rm -rf ./build/$NAME* ./$NAME.jar
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main ${NAME}.java -d build
jar -cvf ${NAME}.jar -C build/ ./
