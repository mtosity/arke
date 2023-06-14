module sassoftware.io/convoy/arke

go 1.19

require (
	github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus v1.1.1
	github.com/armon/go-metrics v0.3.10
	github.com/google/uuid v1.3.0
	github.com/prometheus/client_golang v1.12.0
	github.com/rabbitmq/amqp091-go v1.5.0
	github.com/soheilhy/cmux v0.1.5
	github.com/stretchr/testify v1.8.3
	sassoftware.io/convoy/arke/api v0.0.0-20211213191304-b79d5663ed51
	google.golang.org/grpc v1.43.0
	google.golang.org/protobuf v1.30.0
	sassoftware.io/viya/zlog v0.1.16
	k8s.io/api v0.23.5
	k8s.io/apimachinery v0.23.5
	k8s.io/client-go v0.23.5
)

require (
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.10.0 // indirect
	github.com/go-logr/logr v1.2.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/google/go-cmp v0.5.8 // indirect
	github.com/google/gofuzz v1.1.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/gotnospirit/makeplural v0.0.0-20180622080156-a5f48d94d976 // indirect
	github.com/gotnospirit/messageformat v0.0.0-20190719172517-c1d0bdacdea2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/imdario/mergo v0.3.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/magiconair/properties v1.8.4 // indirect
	github.com/mattn/go-colorable v0.1.8 // indirect
	github.com/mattn/go-isatty v0.0.19 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/zerolog v1.21.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/oauth2 v0.0.0-20210819190943-2bc19b11175f // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/term v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210402141018-6c239bbf2bb1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	k8s.io/klog/v2 v2.30.0 // indirect
	k8s.io/kube-openapi v0.0.0-20211115234752-e816edb12b65 // indirect
	k8s.io/utils v0.0.0-20211116205334-6203023598ed // indirect
	nhooyr.io/websocket v1.8.7 // indirect
	sigs.k8s.io/json v0.0.0-20211020170558-c049b76a60c6 // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.2.1 // indirect
	sigs.k8s.io/yaml v1.2.0 // indirect
)

// NGMTS-21506: fix for CVE-2020-14040
replace (
	github.com/gin-gonic/gin => github.com/gin-gonic/gin v1.9.1
	github.com/gogo/protobuf v1.1.1 => github.com/gogo/protobuf v1.3.2
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.12.0
	sassoftware.io/convoy/arke/api => ./api
	golang.org/x/text v0.3.0 => golang.org/x/text v0.3.8
	golang.org/x/text v0.3.2 => golang.org/x/text v0.3.8
	golang.org/x/text v0.3.7 => golang.org/x/text v0.3.8
)

replace golang.org/x/net => golang.org/x/net v0.9.0
