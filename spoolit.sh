#/bin/bash

if [ $# -lt 1 ]
then
   echo "Please give configuration file as argument, eg: $0 config.ini"
   exit 1
fi

INIFILE=$1
HOME_DIR=`pwd`
MYSQL_LIB=$HOME_DIR/lib/mysql-connector-java-5.0.4-bin.jar
ORACLE_LIB=$HOME_DIR/lib/ojdbc14_g.jar
CLASSPATH=$MYSQL_LIB:$ORACLE_LIB:$HOME_DIR

export CLASSPATH

java SpoolIt $INIFILE

exit 0
