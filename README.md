# Lambda for Section 1: API of Globant’s Data Engineering Coding Challenge

## Prerequisites
* Download [AWS CLI](https://docs.aws.amazon.com/es_es/cli/latest/userguide/getting-started-install.html) 
* Download [Rancher Desktop](https://rancherdesktop.io/)

## AWS Steps
> AWS page
* Create a AWS Profile.
* Create a AWS Lambda role.
* Create a AWS Stack name.
* Create a Private repository on Elastic Container Registry.
> AWS CLI
* Configure AWS CLI with your credentials.


## Instructions to deploy the API Docker image to AWS Lambda
> PowerShell
* Configure the file `\.aws\production.cfg` with required elements setted in AWS steps.
* Execute the script: `.\deploy.sh production full`.
* Verify the process and the Lambda was uploaded sucessfuly.
