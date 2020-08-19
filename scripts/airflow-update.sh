#!/usr/bin/env bash

set -ex

hub clone tulibraries/ansible-playbook-airflow

pushd $PWD/ansible-playbook-airflow

TULFLOW_VERSION=$(echo $TRAVIS_TAG | sed s/^v//)

AIRFLOW_BRANCH=update-tulflow-$TULFLOW_VERSION

PR_MESSAGE="Update tulflow version ($TULFLOW_VERSION)."

hub checkout -b $AIRFLOW_BRANCH

sed -i "s/tulflow..\([0-9]*\.[0-9]*\.[0-9]*\)/tulflow==$TULFLOW_VERSION/g" .env

hub add -u .

hub commit -m "$PR_MESSAGE"

hub push origin $AIRFLOW_BRANCH

hub pull-request -m "$PR_MESSAGE"

popd
