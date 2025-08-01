module github.com/Cookie579/fin-guard

go 1.24

require (
	github.com/aws/aws-sdk-go v1.55.7 // S3 uploader
	github.com/google/uuid v1.6.0 // UUIDs for S3 object keys
)

require github.com/jmespath/go-jmespath v0.4.0 // indirect; indirect – pulled by AWS SDK
