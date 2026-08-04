module github.com/mildlybrutal/overcast/services/processing

go 1.26.4

require (
	github.com/jackc/pgx/v5 v5.8.0
	github.com/mildlybrutal/overcast/gen v0.0.0
	github.com/mildlybrutal/overcast/internal/models v0.0.0
	github.com/redis/go-redis/v9 v9.21.0
	github.com/segmentio/kafka-go v0.4.51
	golang.org/x/sync v0.21.0
	golang.org/x/text v0.38.0
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace (
	github.com/mildlybrutal/overcast/gen => ../../gen
	github.com/mildlybrutal/overcast/internal/models => ../../internal/models
)
