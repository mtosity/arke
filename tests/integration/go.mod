module sassoftware.io/viya/arke/tests/integration

go 1.23.5

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.7.1
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.9.0
	google.golang.org/grpc v1.70.0
	gopkg.in/yaml.v2 v2.4.0
	sassoftware.io/viya/arke v1.25.0
	sassoftware.io/viya/arke/api v1.4.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.7.0 // indirect
	github.com/Azure/go-amqp v1.0.5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.33.0 // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241202173237-19429a94021a // indirect
	google.golang.org/protobuf v1.35.2 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace sassoftware.io/viya/arke => ../../

replace sassoftware.io/viya/arke/api => ../../api
