#!/bin/bash
#
# Install task software and initialise ruby environment
#
# usage: tarfile tmpdir ruby_ver
#
TAR_FILE=$1
TMP_DIR=$2
RUBY_VER=$3

mkdir $TMP_DIR
cd $TMP_DIR
tar zxf $TAR_FILE
if [ ! -e /usr/local/rvm/environments/$RUBY_VER ]; then
  echo "STATUS: installing.${RUBY_VER}"
  /usr/local/rvm/bin/rvm install --autolibs=4 $RUBY_VER
fi
echo "STATUS: installing.gems"
source /usr/local/rvm/environments/$RUBY_VER
gem install bundler
bundle package --all
