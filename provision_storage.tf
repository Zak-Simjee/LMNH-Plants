# Sets up AWs details from variables.tf - MAKE SURE varibles.tf IS IN GITIGNORE
provider "aws" {
    access_key = "${var.aws_access_key}"
    secret_key = "${var.aws_secret_key}"
    region = "${var.region}"
}


resource "aws_s3_bucket" "t3-transform-bucket" {
  bucket = "t3-transform-bucket"

}

# Sets up a free-tier postgres RDS w/ password
resource "aws_db_instance" "t3-output-rds" {
  identifier             = "t3-output-rds"
  instance_class         = "db.t3.micro"
  allocated_storage      = 5
  engine                 = "postgres"
  username               = "postgres"
  password               = "${var.db_password}"
  publicly_accessible    = true
  skip_final_snapshot    = true
  db_subnet_group_name   = "c7-public-db-subnet-group"
  vpc_security_group_ids = ["sg-01745c9fa38b8ed68"]
  provisioner "local-exec" {

    command = "psql -h ${aws_db_instance.t3-output-rds.address} -p 5432 -U \"postgres\" -d \"postgres\" -f \"schema.sql\""

    environment = {
      PGPASSWORD = "${var.db_password}"
    }
  }
}
