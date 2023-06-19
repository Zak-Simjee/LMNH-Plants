# DE-Week-13

This repository contains Python and Terraform code designed to create a data pipeline on AWS for plant sensors at the Liverpool Natural History Museum.

## Installation

Terraform, Python and Docker are required for this project and must be installed prior to using code from this project. If using this project from scratch, a docker image for each module (extract, transform, load) must be built and pushed to the AWS ECR. Both the docker images and ECR repositories must be under the following names: "t3-ecr-extract", "t3-ecr-transform", "t3-ecr-load" and have the corresponding module images uploaded to them. Each AWS Lambda function (t3-extract-lambda, t3-transform-lambda, t3-load-lambda) also requires the correct environment variables to be set in their configuration. This is further detailed in each module directory. The names given for ECR repositories and Lambda functions only apply if these are unchanged in the terraform .tf files.

## Usage

Run the following shell commands in the root directory:

terraform init

terraform apply
