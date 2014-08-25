#!/bin/bash
set +x
set -e
#Check if python, git, pip, java are available
check_for_command() {
    if hash ${COMMAND} 2>/dev/null; then
        continue
    else
        echo "${COMMAND} not available. Please install or configure."
        exit 1
    fi
}

COMMAND='git'
check_for_command

COMMAND='python'
check_for_command

COMMAND='pip'
check_for_command

COMMAND='java'
check_for_command

COMMAND='brew'
check_for_command

#Checkout ccm and dtests
#If this is in dtests, why are we checking it out

pull_or_clone()
{
  if [ -d ${REPO}/.git ]; then
    (
      echo "Pulling git remote:  ${REMOTE}"
      cd ${REPO}
      git checkout ${BRANCH}
      git pull
    )
  else
    echo "Cloning git remote:  ${REMOTE}"
    git clone ${REMOTE} ${REPO}
    (
      cd ${REPO}
      # track desired branch, if it is not the clone default ref
      defaultref=$( git branch | grep '^*' | awk '{print $2}' )
      if [ "${defaultref}" != "${BRANCH}" ]; then
        git checkout --track origin/${BRANCH}
      fi
    )
  fi
}

REPO="ccm"
BRANCH="master"
REMOTE="https://github.com/pcmanus/ccm.git"
pull_or_clone

REPO="cassandra-dtest"
BRANCH="install-scripts"
REMOTE="https://github.com/riptano/cassandra-dtest.git"
pull_or_clone

REPO="cassandra-dbapi2"
BRANCH="master"
REMOTE="https://github.com/mshuler/cassandra-dbapi2.git"
pull_or_clone

REPO="pycassa"
BRANCH="master"
REMOTE="https://github.com/pycassa/pycassa.git"
pull_or_clone

#Check for all dependencies
#Install if possible
pip install --user cassandra-driver
pip install --user nose
pip install --user decorator
echo "Installed all python dependencies"

brew install ant
echo "Installed all java dependencies"

#Install ccm and dbapi2 via pip
pip install --user -e ccm
pip install --user -e cassandra-dbapi2
pip install --user -e pycassa

#Run script that sets loopbacks
sudo ifconfig lo0 alias 127.0.0.2 up
sudo ifconfig lo0 alias 127.0.0.3 up
sudo ifconfig lo0 alias 127.0.0.4 up
sudo ifconfig lo0 alias 127.0.0.5 up

#Sanity Check, run simple_bootstrap_test
CASSANDRA_VERSION=2.0.9 PRINT_DEBUG=true nosetests -s -v cassandra-dtest/bootstrap_test.py:TestBootstrap.simple_bootstrap_test

echo "If you are running Cassandra from a git checkout,"
echo "be sure to set the env var CASSANDRA_DIR"