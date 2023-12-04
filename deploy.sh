#!/bin/bash
# deploys sam based lambda
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DIR_YAML=..
BASE_ZIP_PATH=.
BASE_SRC_PATH=../..

# Adds cfg [production, development, research, staging]
source .aws/$1.cfg

docker build --build-arg APP_NAME=${CONTAINER_NAME} -t ${CONTAINER_NAME} .

aws ecr get-login-password --profile ${AWS_PROFILE} | docker login --username AWS --password-stdin ${CONTAINER_REPOSITORY_URL}

docker tag ${CONTAINER_NAME} ${CONTAINER_REPOSITORY_URL}/${CONTAINER_REPOSITORY_NAME}:${CONTAINER_NAME}

docker push ${CONTAINER_REPOSITORY_URL}/${CONTAINER_REPOSITORY_NAME}:${CONTAINER_NAME}

if [ $# == 2 ] && [ $2 == full ] ; then
    # removing stack
    echo "aws cf delete-stack"
    aws cloudformation delete-stack --stack-name ${STACK_NAME} --profile ${AWS_PROFILE}
    echo "aws cf wait"
    aws cloudformation wait stack-delete-complete --stack-name ${STACK_NAME} --profile ${AWS_PROFILE}
    # deploying stack
    echo "aws cf deploy"
    aws cloudformation deploy \
       --template-file ${DIR_YAML}/globant-section1-api-lambda/lambda_api.yaml \
       --stack-name ${STACK_NAME} \
       --parameter-overrides \
       ContainerName=${CONTAINER_NAME} \
       ImageURI=${CONTAINER_REPOSITORY_URL}/${CONTAINER_REPOSITORY_NAME}:${CONTAINER_NAME} \
       LambdaRole=${AWS_LAMBDA_ROLE} \
       Environment=${ENV} \
       --profile ${AWS_PROFILE}
fi
