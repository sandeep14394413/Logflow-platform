# ── LogFlow EKS Infrastructure ───────────────────────────────────────────────
# Creates a production-grade EKS cluster with managed node groups,
# cluster addons, IAM roles, and supporting networking.

terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.0"
    }
  }
  backend "s3" {
    bucket         = "logflow-terraform-state"
    key            = "eks/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "logflow-terraform-lock"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "logflow"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ── Data ─────────────────────────────────────────────────────────────────────
data "aws_availability_zones" "available" { state = "available" }

data "aws_caller_identity" "current" {}

# ── VPC ───────────────────────────────────────────────────────────────────────
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "logflow-${var.environment}"
  cidr = var.vpc_cidr

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = var.private_subnet_cidrs
  public_subnets  = var.public_subnet_cidrs
  intra_subnets   = var.intra_subnet_cidrs

  enable_nat_gateway     = true
  single_nat_gateway     = var.environment != "production"
  one_nat_gateway_per_az = var.environment == "production"

  enable_dns_hostnames = true
  enable_dns_support   = true

  # Required tags for EKS subnet discovery.
  public_subnet_tags = {
    "kubernetes.io/role/elb"                    = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb"            = "1"
    "kubernetes.io/cluster/${local.cluster_name}" = "shared"
  }
}

# ── EKS Cluster ───────────────────────────────────────────────────────────────
locals {
  cluster_name = "logflow-${var.environment}"
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = local.cluster_name
  cluster_version = var.k8s_version

  vpc_id                   = module.vpc.vpc_id
  subnet_ids               = module.vpc.private_subnets
  control_plane_subnet_ids = module.vpc.intra_subnets

  cluster_endpoint_public_access  = true
  cluster_endpoint_private_access = true

  # Cluster addons managed by EKS.
  cluster_addons = {
    coredns = {
      most_recent = true
      configuration_values = jsonencode({
        replicaCount = 3
        resources = {
          limits   = { cpu = "200m", memory = "170Mi" }
          requests = { cpu = "100m", memory = "70Mi" }
        }
      })
    }
    kube-proxy   = { most_recent = true }
    vpc-cni      = { most_recent = true }
    aws-ebs-csi-driver = {
      most_recent              = true
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  # ── Managed node groups ───────────────────────────────────────────────────
  eks_managed_node_groups = {

    # System nodes: kube-system + monitoring.
    system = {
      name           = "system"
      instance_types = ["m6i.large"]
      min_size       = 2
      max_size       = 5
      desired_size   = 3
      labels = {
        role = "system"
      }
      taints = [{
        key    = "dedicated"
        value  = "system"
        effect = "NO_SCHEDULE"
      }]
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 50
            volume_type           = "gp3"
            iops                  = 3000
            throughput            = 125
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }

    # Application nodes: logflow microservices.
    application = {
      name           = "application"
      instance_types = ["c6i.2xlarge", "c6a.2xlarge"]
      min_size       = 3
      max_size       = 30
      desired_size   = 6
      capacity_type  = "SPOT"
      labels = {
        role = "application"
      }
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 100
            volume_type           = "gp3"
            iops                  = 6000
            throughput            = 250
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }

    # Storage nodes: ClickHouse + Kafka (NVMe optimised).
    storage = {
      name           = "storage"
      instance_types = ["r6i.2xlarge", "r6a.2xlarge"]
      min_size       = 3
      max_size       = 12
      desired_size   = 3
      capacity_type  = "ON_DEMAND"
      labels = {
        role = "storage"
      }
      taints = [{
        key    = "dedicated"
        value  = "storage"
        effect = "NO_SCHEDULE"
      }]
      block_device_mappings = {
        xvda = {
          device_name = "/dev/xvda"
          ebs = {
            volume_size           = 500
            volume_type           = "gp3"
            iops                  = 16000
            throughput            = 1000
            encrypted             = true
            delete_on_termination = true
          }
        }
      }
    }
  }

  # ── Security groups ───────────────────────────────────────────────────────
  node_security_group_additional_rules = {
    ingress_self_all = {
      description = "Node to node all ports/protocols"
      protocol    = "-1"
      from_port   = 0
      to_port     = 0
      type        = "ingress"
      self        = true
    }
    egress_all = {
      description      = "Node all egress"
      protocol         = "-1"
      from_port        = 0
      to_port          = 0
      type             = "egress"
      cidr_blocks      = ["0.0.0.0/0"]
      ipv6_cidr_blocks = ["::/0"]
    }
  }
}

# ── IRSA: EBS CSI Driver ──────────────────────────────────────────────────────
module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${local.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    ex = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}

# ── gp3 StorageClass ──────────────────────────────────────────────────────────
resource "kubernetes_storage_class" "gp3" {
  metadata {
    name = "gp3"
    annotations = {
      "storageclass.kubernetes.io/is-default-class" = "true"
    }
  }
  storage_provisioner    = "ebs.csi.aws.com"
  volume_binding_mode    = "WaitForFirstConsumer"
  allow_volume_expansion = true
  parameters = {
    type      = "gp3"
    iops      = "6000"
    throughput = "250"
    encrypted = "true"
  }
}

# ── Outputs ───────────────────────────────────────────────────────────────────
output "cluster_name" {
  value = module.eks.cluster_name
}
output "cluster_endpoint" {
  value     = module.eks.cluster_endpoint
  sensitive = true
}
output "configure_kubectl" {
  value = "aws eks update-kubeconfig --region ${var.aws_region} --name ${module.eks.cluster_name}"
}
output "vpc_id" {
  value = module.vpc.vpc_id
}
