module github.com/ONSdigital/dp-kafka/v3

go 1.18

replace (
	// solves CVE-2021-3121
	github.com/prometheus/common => github.com/prometheus/common v0.34.0
	// solves sonatype-2020-0584 CWE-79: Improper Neutralization of Input During Web Page Generation ('Cross-site Scripting') github.com/yuin/goldmark - Cross-Site Scripting (XSS)
	github.com/yuin/goldmark => github.com/yuin/goldmark v1.4.12
)

require (
	github.com/ONSdigital/dp-healthcheck v1.4.0-beta.1
	github.com/ONSdigital/log.go/v2 v2.3.0-beta
	github.com/Shopify/sarama v1.34.1
	github.com/go-avro/avro v0.0.0-20171219232920-444163702c11
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/pkg/errors v0.9.1
	github.com/smartystreets/goconvey v1.7.2
)

require (
	github.com/ONSdigital/dp-api-clients-go/v2 v2.160.0-beta // indirect
	github.com/ONSdigital/dp-net/v2 v2.5.0-beta // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/eapache/go-resiliency v1.3.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hokaccha/go-prettyjson v0.0.0-20211117102719-0474bc63780f // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/compress v1.15.8 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475
	github.com/smartystreets/assertions v1.13.0 // indirect
	golang.org/x/crypto v0.0.0-20220622213112-05595931fe9d // indirect
	golang.org/x/net v0.0.0-20220708220712-1185a9018129 // indirect
	golang.org/x/sys v0.0.0-20220715151400-c0bba94af5f8 // indirect
	gopkg.in/avro.v0 v0.0.0-20171217001914-a730b5802183 // indirect
)
