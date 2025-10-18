module github.com/Cookie579/fin-guard

go 1.24

require (
	github.com/aws/aws-sdk-go v1.55.8 // S3 uploader
	github.com/aws/aws-sdk-go-v2/config v1.30.3
	github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue v1.20.15
	github.com/aws/aws-sdk-go-v2/service/dynamodb v1.51.1
	github.com/aws/aws-sdk-go-v2/service/kinesis v1.32.18
	github.com/google/uuid v1.6.0 // UUIDs for S3 object keys
	github.com/redis/go-redis/v9 v9.13.0

)

require github.com/jmespath/go-jmespath v0.4.0 // indirect; indirect – pulled by AWS SDK

require github.com/aws/aws-sdk-go-v2 v1.39.3

require (
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.6.8 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.3 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.2 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.10 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/dynamodbstreams v1.31.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/endpoint-discovery v1.11.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.27.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.32.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.36.0 // indirect
	github.com/aws/smithy-go v1.23.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
)
