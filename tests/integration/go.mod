module sassoftware.io/viya/arke/tests/integration

go 1.22.6

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.7.1
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.9.0
	google.golang.org/grpc v1.65.0
	sassoftware.io/viya/arke v1.24.4
	sassoftware.io/viya/arke/api v1.3.0
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.11.1 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.7.0 // indirect
	github.com/Azure/go-amqp v1.0.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/protobuf v1.34.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace sassoftware.io/viya/arke => ../../
