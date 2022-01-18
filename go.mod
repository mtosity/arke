module sassoftware.io/convoy/arke

go 1.16

require (
	github.com/Azure/azure-amqp-common-go/v3 v3.2.1
	github.com/Azure/azure-service-bus-go v0.11.5
	github.com/armon/go-metrics v0.3.10
	github.com/fatih/color v1.10.0 // indirect
	github.com/google/uuid v1.3.0
	github.com/gotnospirit/messageformat v0.0.0-20190719172517-c1d0bdacdea2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/magiconair/properties v1.8.4 // indirect
	github.com/prometheus/client_golang v1.11.0
	github.com/soheilhy/cmux v0.1.5
	github.com/streadway/amqp v1.0.0
	github.com/stretchr/testify v1.7.0
	sassoftware.io/convoy/arke/api v0.0.0-20211213191304-b79d5663ed51
	google.golang.org/genproto v0.0.0-20210212180131-e7f2df4ecc2d // indirect
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.27.1
	sassoftware.io/viya/zlog v0.1.14
	k8s.io/api v0.22.3
	k8s.io/apimachinery v0.22.3
	k8s.io/client-go v0.22.3
)

// NGMTS-21506: fix for CVE-2020-14040
replace (
	github.com/gin-gonic/gin v1.6.3 => github.com/gin-gonic/gin v1.7.3
	golang.org/x/text v0.3.0 => golang.org/x/text v0.3.7
	golang.org/x/text v0.3.2 => golang.org/x/text v0.3.7
)
