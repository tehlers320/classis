package main

import (
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"time"
)

// KWriter sends metrics to a kinesis stream
type KWriter struct {
	Client *kinesis.Kinesis
}

// @todo: Each shard can support up to 1,000 records per second for writes, up to a
// @todo: maximum total data write rate of 1 MB per second (including partition keys).
// @todo: This write limit applies to operations such as PutRecord and PutRecords.
func (k *KWriter) Write(p []byte) (n int, err error) {

	// Partition by hashing the data. This will be a bit random, but will at least ensure all shards are used
	// (if we ever have more than one)
	partitionKey := fmt.Sprintf("%x", md5.Sum(p))

	// Try a few times on error. The initial reason for this is Go AWS SDK seems to have some weird timing issue,
	// where sometimes the request would just EOF if requests are made in regular intervals. For example doing
	// "put-record" from us-west-1 to ap-southeast-2 every 6-7 seconds will cause EOF error, without the record being sent.
	for i, backOff := 0, time.Second; i < maxRetries; i, backOff = i+1, backOff*2 {
		_, err := k.Client.PutRecord(&kinesis.PutRecordInput{
			Data:         p,
			PartitionKey: aws.String(partitionKey),
			StreamName:   aws.String(awsStreamName),
		})

		if err == nil {
			return len(p), nil
		}

		// Send has failed.
		if i < maxRetries-1 {
			fmt.Printf("Retrying in %d s, sender failed to put record on try %d: %s.\n", backOff/time.Second, i, err)
			time.Sleep(backOff)
		} else {
			fmt.Printf("Aborting, sender failed to put record on try %d: %s.\n", i, err)
		}
	}
	// @todo: return error from above instead
	return len(p), nil
}
