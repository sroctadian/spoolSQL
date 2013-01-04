#/bin/bash

if [ $# -lt 1 ]
then
   echo "Please give configuration file as argument, eg: $0 config.ini"
   exit 1
fi

INIFILE=$1
HOME_DIR=`pwd`
export CLASSPATH=.:$HOME_DIR/lib/mysql-connector-java-5.0.4-bin.jar:$HOME_DIR/lib/ojdbc14_g.jar:$HOME_DIR/lib/tdgssconfig.jar:$HOME_DIR/lib/terajdbc4.jar

java SpoolIt $INIFILE

exit 0
