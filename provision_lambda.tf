# This section handles setting Lambda permissions and roles
data "aws_iam_policy_document" "AWSLambdaTrustPolicy" {
  statement {
    actions    = ["sts:AssumeRole"]
    effect     = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}
resource "aws_iam_role" "terraform_function_role" {
  name               = "terraform_function_role"
  assume_role_policy = "${data.aws_iam_policy_document.AWSLambdaTrustPolicy.json}"
}


resource "aws_iam_role" "step_function_role" {
  name               = "step_function_role"
  assume_role_policy = <<-EOF
  {
    "Statement": [
      {
        "Action": "sts:AssumeRole",
        "Principal": {
          "Service": "states.amazonaws.com"
        },
        "Effect": "Allow",
        "Sid": "StepFunctionAssumeRole"
      }
    ]
  }
  EOF
}

resource "aws_iam_role_policy" "step_function_policy" {
  name    = "step_function_policy"
  role    = aws_iam_role.step_function_role.id

  policy  = <<-EOF
  {
    "Statement": [
      {
        "Action": [
          "lambda:InvokeFunction"
        ],
        "Effect": "Allow",
        "Resource": "arn:aws:lambda:eu-west-2:605126261673:function:*"
      }
    ]
  }
  EOF
}



# # This section sets up ECR and runs docker 
data "aws_ecr_repository" "t3-ecr-extract" {
  name = "${var.ecr_extract}"
  # image_tag_mutability = "MUTABLE"
}
data "aws_ecr_repository" "t3-ecr-transform" {
  name = "${var.ecr_transform}"
  # image_tag_mutability = "MUTABLE"
}
data "aws_ecr_repository" "t3-ecr-load" {
  name = "${var.ecr_load}"
  # image_tag_mutability = "MUTABLE"
}




# This section creates the lambda functions from the ECR images
resource "aws_lambda_function" "extract_function" {
    function_name = "t3-extract-lambda"
    image_uri     = "${data.aws_ecr_repository.t3-ecr-extract.repository_url}:latest"
    package_type  = "Image"
    role = "${aws_iam_role.terraform_function_role.arn}"
    memory_size      = 1024
    timeout          = 50
    environment {
      variables = {
        DATABASE_USERNAME="postgres"
        DATABASE_PASSWORD=var.db_password
        DATABASE_IP= aws_db_instance.t3-output-rds.address
        DATABASE_PORT="5432"
        DATABASE_NAME="plant"
        ACCESS_KEY=var.aws_access_key
        SECRET_KEY=var.aws_secret_key
        SCHEMA_NAME="raw"
    }
  }
}
resource "aws_lambda_function" "transform_function" {
    function_name = "t3-transform-lambda"
    image_uri     = "${data.aws_ecr_repository.t3-ecr-transform.repository_url}:latest"
    package_type  = "Image"
    role = "${aws_iam_role.terraform_function_role.arn}"
    memory_size      = 1024
    timeout          = 50
    environment {
      variables = {
        DATABASE_USERNAME="postgres"
        DATABASE_PASSWORD=var.db_password
        DATABASE_IP= aws_db_instance.t3-output-rds.address
        DATABASE_PORT="5432"
        DATABASE_NAME="plant"
        ACCESS_KEY=var.aws_access_key
        SECRET_KEY=var.aws_secret_key
    }
  }
}
resource "aws_lambda_function" "load_function" {
    function_name = "t3-load-lambda"
    image_uri     = "${data.aws_ecr_repository.t3-ecr-load.repository_url}:latest"
    package_type  = "Image"
    role = "${aws_iam_role.terraform_function_role.arn}"
    memory_size      = 1024
    timeout          = 50
    environment {
      variables = {
        DATABASE_USERNAME="postgres"
        DATABASE_PASSWORD=var.db_password
        DATABASE_IP= aws_db_instance.t3-output-rds.address
        DATABASE_PORT="5432"
        DATABASE_NAME="plant"
        ACCESS_KEY=var.aws_access_key
        SECRET_KEY=var.aws_secret_key
    }
  }
    
}

# This section combines the lambda's into a step function
resource "aws_sfn_state_machine" "t3-state-machine" {
  name     = "t3-test-state-machine"
  role_arn = "${aws_iam_role.step_function_role.arn}"

  definition = <<EOF
{
  "Comment": "State machine for the plant group project",
  "StartAt": "Lambda Extract",
  "States": {
    "Lambda Extract": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.extract_function.arn}",
      "Next": "Lambda Transform"
    },
    "Lambda Transform": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.transform_function.arn}",
      "Next": "Lambda Load"
    },
    "Lambda Load": {
      "Type": "Task",
      "Resource": "${aws_lambda_function.load_function.arn}",
      "End": true
    }
  }
}

EOF
}

