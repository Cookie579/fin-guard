# ============================================================
# AWS WAF Rate Limiting for FinGuard API
# ============================================================
# Attaches WAF to ALB/Ingress to rate-limit at the edge,
# so pods don't even receive excess traffic.
#
# Rate limits:
# - /api/orchestrator/*: 300 requests per 5 minutes per IP
# - General: 1000 requests per 5 minutes per IP
# ============================================================

resource "aws_wafv2_web_acl" "finguard_api" {
  name        = "finguard-api-rate-limit"
  description = "Rate limiting for FinGuard API endpoints"
  scope       = "REGIONAL"  # For ALB; use CLOUDFRONT for CloudFront

  default_action {
    allow {}
  }

  # ============================================================
  # Rule 1: Rate limit /api/orchestrator/* (strict)
  # ============================================================
  rule {
    name     = "rate-limit-orchestrator"
    priority = 1

    override_action {
      none {}
    }

    statement {
      rate_based_statement {
        limit              = 300  # 300 requests per 5 minutes
        aggregate_key_type = "IP"

        # Scope-down: only count requests to /api/orchestrator/*
        scope_down_statement {
          byte_match_statement {
            search_string         = "/api/orchestrator/"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "LOWERCASE"
            }
            positional_constraint = "STARTS_WITH"
          }
        }
      }
    }

    action {
      block {
        custom_response {
          response_code = 429
          custom_response_body_key = "rate-limit-exceeded"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "finguard-orchestrator-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  # ============================================================
  # Rule 2: Rate limit /api/* (general)
  # ============================================================
  rule {
    name     = "rate-limit-api-general"
    priority = 2

    override_action {
      none {}
    }

    statement {
      rate_based_statement {
        limit              = 1000  # 1000 requests per 5 minutes
        aggregate_key_type = "IP"

        scope_down_statement {
          byte_match_statement {
            search_string         = "/api/"
            field_to_match {
              uri_path {}
            }
            text_transformation {
              priority = 0
              type     = "LOWERCASE"
            }
            positional_constraint = "STARTS_WITH"
          }
        }
      }
    }

    action {
      block {
        custom_response {
          response_code = 429
          custom_response_body_key = "rate-limit-exceeded"
        }
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "finguard-api-rate-limit"
      sampled_requests_enabled   = true
    }
  }

  # ============================================================
  # Rule 3: Block known bad bots
  # ============================================================
  rule {
    name     = "block-bad-bots"
    priority = 3

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesBotControlRuleSet"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "finguard-bad-bots"
      sampled_requests_enabled   = true
    }
  }

  # ============================================================
  # Rule 4: AWS Common Rule Set (OWASP protections)
  # ============================================================
  rule {
    name     = "aws-common-rules"
    priority = 4

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        vendor_name = "AWS"
        name        = "AWSManagedRulesCommonRuleSet"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "finguard-common-rules"
      sampled_requests_enabled   = true
    }
  }

  # Custom response body for rate limiting
  custom_response_body {
    key          = "rate-limit-exceeded"
    content      = "{\"error\": \"RATE_LIMIT_EXCEEDED\", \"message\": \"Too many requests. Please slow down.\", \"retry_after_seconds\": 300}"
    content_type = "APPLICATION_JSON"
  }

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = "finguard-waf"
    sampled_requests_enabled   = true
  }

  tags = {
    Environment = var.environment
    Application = "finguard"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# Associate WAF with ALB
# ============================================================
resource "aws_wafv2_web_acl_association" "finguard_alb" {
  resource_arn = var.alb_arn  # Your ALB ARN
  web_acl_arn  = aws_wafv2_web_acl.finguard_api.arn
}

# ============================================================
# CloudWatch Logging for WAF
# ============================================================
resource "aws_wafv2_web_acl_logging_configuration" "finguard" {
  log_destination_configs = [aws_cloudwatch_log_group.waf_logs.arn]
  resource_arn            = aws_wafv2_web_acl.finguard_api.arn

  redacted_fields {
    single_header {
      name = "authorization"
    }
  }
}

resource "aws_cloudwatch_log_group" "waf_logs" {
  name              = "aws-waf-logs-finguard"
  retention_in_days = 30

  tags = {
    Application = "finguard"
    ManagedBy   = "terraform"
  }
}

# ============================================================
# Variables
# ============================================================
variable "alb_arn" {
  description = "ARN of the ALB to attach WAF to"
  type        = string
}
