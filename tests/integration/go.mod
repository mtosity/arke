module sassoftware.io/viya/arke/tests/integration

go 1.23.5

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.7.1
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.10.0
	google.golang.org/grpc v1.71.0
	gopkg.in/yaml.v2 v2.4.0
	sassoftware.io/viya/arke v1.27.0
	sassoftware.io/viya/arke/api v1.7.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.7.0 // indirect
	github.com/Azure/go-amqp v1.0.5 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	golang.org/x/net v0.34.0 // indirect
	golang.org/x/sys v0.29.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250115164207-1a7da9e5054f // indirect
	google.golang.org/protobuf v1.36.5 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace sassoftware.io/viya/arke => ../../

replace sassoftware.io/viya/arke/api => ../../api
