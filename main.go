package main

// classis fetches metrics from AWS and pushes them in a graphite format to kinesis
// usage: classis --region ap-southeast-2 --stream-name kinesis-stream-name --arn arn:aws:iam::111111111111:role/role_name

// @todo: sort out proper logging and error handling / reporting
// @todo: write proper README with necessary role permissions

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	awsArn        *string // the ARN uri for the role to assume
	awsRegion     *string // the region to use for assuming the role
	awsStreamName *string // the Kinesis stream name
)

// regions are all the available AWS regions, not sure if there is an API to fetch them
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

	awsArn = flag.String("arn", "", "REQUIRED: ARN of the role to assume")
	awsRegion = flag.String("region", "", "REQUIRED: Region for the AWS kinesis stream")
	awsStreamName = flag.String("stream-name", "", "REQUIRED: Kinesis stream name")
	flag.Parse()

	if *awsRegion == "" {
		log.Fatal("Provide region via --region flag")
	}

	if *awsStreamName == "" {
		log.Fatal("Provide Kinesis stream name via --stream-name flag")
	}

	if *awsArn == "" {
		log.Fatal("Provide ARN role via --arn flag")
	}

	kWriter := NewKWriter(awsArn, awsRegion, awsStreamName, 3)

	typeGatherer := TypeGatherer(awsArn)

	// keep a buffer of roughly 1mb if every metric is around 60bytes
	// 60 * 1,024 * 16 = 0.94mb
	// if we are fetching 100 metrics per minute this buffer should last for
	// 16,384 / 100 = 163.84 min ~= 2hr 43 minutes
	metrics := NewMetrics(kWriter, 1024*16)

	// metrics gatherers should push metrics to this channel
	metricSink := make(chan string, 1024)
	// fetch new metrics this often
	fetchTicker := time.NewTicker(time.Second * 45)
	// put metrics to kinesis this often
	sendTicker := time.NewTicker(time.Second * 10)
	// send an os.Signal to this channel to stop the program
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill)

	// gather metrics from AWS and push them to the metricSink
	go func() {
		for {
			typeGatherer.Gather(metricSink)
			<-fetchTicker.C
		}
	}()

	// pump metrics from the metric sink channel to the Metrics struct
	go func() {
		for {
			metric := <-metricSink
			metrics.Add(metric)
		}
	}()

	// send the currently gathered metrics to Kinesis
	go func() {
		for {
			if _, err := metrics.Send(); err != nil {
				log.Fatal(err)
			}
			<-sendTicker.C
		}
	}()

	// block until we receive an OS interrupt
	<-interrupt
	fetchTicker.Stop()
	sendTicker.Stop()
	log.Printf("flushing %d outstanding metrics to kinesis\n", len(metrics.buffer))
	metrics.Send()
	log.Printf("%d metrics was sent to kinesis\n", metrics.MetricsSent)
}

func TypeGatherer(roleARN *string) *TypeMetrics {
	return &TypeMetrics{
		RoleARN:     roleARN,
		Credentials: stscreds.NewCredentials(session.New(&aws.Config{}), *awsArn),
	}
}

type TypeMetrics struct {
	RoleARN     *string
	Credentials *credentials.Credentials
}

// Gather fetches the count of EC2 and RDS instance types and should
// push a string to the out channel in graphite format: "namespace.metrics value timestamp"
// note that there is no need terminating the string with a \n
func (tp *TypeMetrics) Gather(out chan string) {
	for _, regionName := range regions {

		go func(regionName string) {
			tp.instanceTypes(regionName, out)
		}(regionName)

		go func(regionName string) {
			tp.rdsInstanceTypes(regionName, out)
		}(regionName)
	}
}

func (tp *TypeMetrics) instanceTypes(regionName string, out chan string) {

	service := ec2.New(tp.newSession(&regionName))
	result, err := service.DescribeInstances(&ec2.DescribeInstancesInput{})

	if err != nil {
		log.Printf("%s\n", err)
		return
	}

	instTypes := make(map[string]int, 0)
	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			instTypes[*instance.InstanceType]++
		}
	}

	for instType, instCount := range instTypes {
		out <- fmt.Sprintf("aws.%s.instance_types.ec2.%s %d %d", regionName, instType, instCount, int32(time.Now().Unix()))
	}
}

func (tp *TypeMetrics) rdsInstanceTypes(regionName string, out chan string) {

	service := rds.New(tp.newSession(&regionName))
	result, err := service.DescribeDBInstances(&rds.DescribeDBInstancesInput{})

	if err != nil {
		log.Printf("%s\n", err)
		return
	}

	instTypes := make(map[string]int, 0)
	for _, instance := range result.DBInstances {
		instTypes[*instance.DBInstanceClass]++
	}

	for instType, instCount := range instTypes {
		out <- fmt.Sprintf("aws.%s.instance_types.%s %d %d", regionName, instType, instCount, int32(time.Now().Unix()))
	}
}

func (tp *TypeMetrics) newSession(region *string) *session.Session {
	return session.New(&aws.Config{
		Credentials: tp.Credentials,
		Region:      region,
	})
}
