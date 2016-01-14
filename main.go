package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"time"
)

var (
	awsArn        = ""
	awsRegion     = ""
	awsStreamName = ""
	maxRetries    = 3
)

var regions = []string{
	"us-east-1",
	"us-west-2",
	"us-west-1",
	"eu-west-1",
	"eu-central-1",
	"ap-southeast-1",
	"ap-northeast-1",
	"ap-southeast-2",
	"ap-northeast-2",
	"sa-east-1",
}

func main() {

	flag.StringVar(&awsArn, "arn", "", "ARN of the role to assume")
	flag.StringVar(&awsRegion, "region", "", "REQUIRED: Region")
	flag.StringVar(&awsStreamName, "stream-name", "", "REQUIRED: Kinesis stream name")
	flag.Parse()

	if awsRegion == "" {
		log.Fatal("Provide region via --region flag")
	}

	if awsStreamName == "" {
		log.Fatal("Provide Kinesis stream name via --stream-name flag")
	}

	if awsArn == "" {
		log.Fatal("Provide ARN role via --arn flag")
	}

	kWriter := &KWriter{
		Client: kinesis.New(assumeRole(awsArn, awsRegion)),
	}

	// keep a buffer of roughly 1mb if every metric is around 60bytes
	// 60 * 1,024 * 16 = 0.94mb
	// if we are fetching 100 metrics per minute this buffer should last for
	// 16,384 / 100 = 163.84 min ~= 2hr 43 minutes
	metrics := NewMetrics(kWriter, 1024*16)

	metricSink := make(chan string, 1024)
	fetchTicker := time.NewTicker(time.Second * 60)
	sendTicker := time.NewTicker(time.Second * 10)

	go func() {
		for range fetchTicker.C {
			//			log.Printf("fetching")
			perMinute(metricSink)
		}
	}()

	go func() {
		for range sendTicker.C {
			//			log.Printf("sending")
			metrics.Pump()
		}
	}()

	go func() {
		for {
			value := <-metricSink
			//			log.Printf("adding %s", value)
			metrics.Add(value)
		}
	}()

	time.Sleep(time.Hour * 24)
	fetchTicker.Stop()
	sendTicker.Stop()

}

func perMinute(out chan string) {
	for _, regionName := range regions {
		ec2Instances := getEc2Instances(regionName)
		for instanceType, instCount := range sumEc2InstanceTypes(ec2Instances) {
			out <- fmt.Sprintf("aws.%s.instance_types.ec2.%s %d %d", regionName, instanceType, instCount, int32(time.Now().Unix()))
		}

		rdsInstances := getRDSInstances(regionName)
		for rdsType, rdsCount := range sumRdsInstanceTypes(rdsInstances) {
			out <- fmt.Sprintf("aws.%s.instance_types.%s %d %d", regionName, rdsType, rdsCount, int32(time.Now().Unix()))
		}
	}
}

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
			//			fmt.Printf("%s", p)
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

func getEc2Instances(region string) []*ec2.Instance {
	var instances []*ec2.Instance

	service := ec2.New(assumeRole(awsArn, region))
	result, err := service.DescribeInstances(&ec2.DescribeInstancesInput{})
	if err != nil {
		fmt.Printf("Failed to list EC2 instance: %s\n", err)
		return instances
	}
	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			instances = append(instances, instance)
		}
	}
	return instances
}

func sumEc2InstanceTypes(ec2Instances []*ec2.Instance) map[string]int {
	ec2InstanceTypes := make(map[string]int, 0)
	for _, instance := range ec2Instances {
		if *instance.State.Name != "terminated" && *instance.State.Name != "stopped" {
			ec2InstanceTypes[*instance.InstanceType]++
		}
	}
	return ec2InstanceTypes
}

func getRDSInstances(region string) []*rds.DBInstance {
	var instances []*rds.DBInstance
	service := rds.New(assumeRole(awsArn, region))
	result, err := service.DescribeDBInstances(&rds.DescribeDBInstancesInput{})
	if err != nil {
		fmt.Printf("Failed to list RDS instance: %s\n", err)
		return instances
	}

	for _, instance := range result.DBInstances {
		instances = append(instances, instance)
	}
	return instances
}

func sumRdsInstanceTypes(rdsInstances []*rds.DBInstance) map[string]int {
	rdsInstanceTypes := make(map[string]int, 0)
	for _, instance := range rdsInstances {
		rdsInstanceTypes[*instance.DBInstanceClass]++
	}
	return rdsInstanceTypes
}

func assumeRole(roleARN, region string) *session.Session {
	return session.New(&aws.Config{
		Credentials: stscreds.NewCredentials(session.New(&aws.Config{}), roleARN),
		Region:      aws.String(region),
	})
}
