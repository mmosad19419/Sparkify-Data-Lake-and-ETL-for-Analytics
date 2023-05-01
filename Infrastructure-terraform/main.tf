terraform {

  required_providers {
    aws = {
        source = "hashicorp/aws"
        version = "3.74.0"
    }
  }
}

provider "aws" {
    region = var.region
    shared_credentials_file = "C:/Users/original/.terraform/.aws/credentials"
    profile = "udacity"
}

# resource provisioning

resource "aws_emr_cluster" "sparkify-cluster" {
  name = "sparkify-cluster"
  release_label = "emr-6.8.0"
  applications = ["Spark", "Hadoop", "YARN", "Zeppelin"]
  ec2_attributes {
    instance_profile  = aws_iam_instance_profile.EMR_CLUSTER_InstanceProfile.arn
  }
  master_instance_group {
    instance_type = "m3.xlarge"
  }
  core_instance_group {
    instance_count = 3
    instance_type = "m3.xlarge"
  }
  service_role = aws_iam_role.EMR_Role.arn
}

# IAM role for EMR Service
resource "aws_iam_role" "EMR_Role" {
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}


# IAM Role for EC2 Instance Profile
resource "aws_iam_role" "EMR_EC2_Role" {
  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}


# instance profile
resource "aws_iam_instance_profile" "EMR_CLUSTER_InstanceProfile" {
  name = "EMR_CLUSTER_InstanceProfile"
  role = aws_iam_role.EMR_EC2_Role.name
}