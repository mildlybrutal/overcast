module github.com/mildlybrutal/overcast/services/ingestion

go 1.26.4

require (
	github.com/mildlybrutal/overcast/internal/models v0.0.0
	github.com/segmentio/kafka-go v0.4.51
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/mildlybrutal/overcast/gen v0.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)

replace (
	github.com/mildlybrutal/overcast/gen => ../../gen
	github.com/mildlybrutal/overcast/internal/models => ../../internal/models
)
