variable "region" {
  description = "Default region for the aws provider"
  type = string
  default = "us-east-1"
}

variable "aws_access_key" {
  description = "Access key for the aws provider"
  type = string
  default = "****************"
}

variable "aws_secret_access_key" {
  description = "Secret access key for the aws provider"
  type = string
  default = "*******************************"
}

variable "aws_session_token" {
  description = "Session token key for the aws provider"
  type = string
  default = "********************"
}
