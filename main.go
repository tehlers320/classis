package main

// classis fetches metrics from AWS and pushes them in a graphite format to kinesis
// usage: classis --region ap-southeast-2 --stream-name kinesis-stream-name --arn arn:aws:iam::111111111111:role/role_name

// @todo: sort out proper logging and error handling / reporting
// @todo: write proper README with necessary role permissions

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/rds"
	"log"
	"os"
	"os/signal"
	"time"
)

var (
	awsArn        = "" // the ARN uri for the role to assume
	awsRegion     = "" // the region to use for assuming the role
	awsStreamName = "" // the kinesis stream name
	maxRetries    = 3  // try sending kinesis this many times before aborting
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
			gatherInstanceMetrics(metricSink)
			<-fetchTicker.C
		}
	}()

	// pump metrics from the metric sink channel to the Metrics struct
	go func() {
		for {
			value := <-metricSink
			metrics.Add(value)
		}
	}()

	// send the currently gathered metrics to Kinesis
	go func() {
		for {
			metrics.Send()
			<-sendTicker.C
		}
	}()

	// block until we receive an OS interrupt
	<-interrupt
	fetchTicker.Stop()
	log.Printf("flushing outstanding metrics to kinesis\n")
	sendTicker.Stop()
	metrics.Send()
	log.Printf("%d metrics was sent to kinesis\n", metrics.MetricsSent)
}

// assumeRole uses the STS get assume the roleARN role and returns a Session that
// can by used by service clients
func assumeRole(roleARN, region string) *session.Session {
	return session.New(&aws.Config{
		Credentials: stscreds.NewCredentials(session.New(&aws.Config{}), roleARN),
		Region:      aws.String(region),
	})
}

// gatherInstanceMetrics fetches the count of EC2 and RDS instance types and should
// push a string to the out channel in graphite format: "namespace.metrics value timestamp"
// note that there is no need terminating the string with a \n
func gatherInstanceMetrics(out chan string) {
	for _, regionName := range regions {
		ec2Instances, err := getEc2Instances(regionName)
		if err != nil {
			log.Printf("%s\n", err)
		}
		for instanceType, instCount := range sumEc2InstanceTypes(ec2Instances) {
			out <- fmt.Sprintf("aws.%s.instance_types.ec2.%s %d %d", regionName, instanceType, instCount, int32(time.Now().Unix()))
		}

		rdsInstances, err := getRDSInstances(regionName)
		if err != nil {
			log.Printf("%s\n", err)
		}
		for rdsType, rdsCount := range sumRdsInstanceTypes(rdsInstances) {
			out <- fmt.Sprintf("aws.%s.instance_types.%s %d %d", regionName, rdsType, rdsCount, int32(time.Now().Unix()))
		}
	}
}

func getEc2Instances(region string) ([]*ec2.Instance, error) {
	var instances []*ec2.Instance
	service := ec2.New(assumeRole(awsArn, region))
	result, err := service.DescribeInstances(&ec2.DescribeInstancesInput{})
	if err != nil {
		return instances, err
	}
	for _, reservation := range result.Reservations {
		for _, instance := range reservation.Instances {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func getRDSInstances(region string) ([]*rds.DBInstance, error) {
	var instances []*rds.DBInstance
	service := rds.New(assumeRole(awsArn, region))
	result, err := service.DescribeDBInstances(&rds.DescribeDBInstancesInput{})
	if err != nil {
		return instances, err
	}
	for _, instance := range result.DBInstances {
		instances = append(instances, instance)
	}
	return instances, nil
}

// sumEc2InstanceTypes returns a count of EC2 instances that AWS will charge for
func sumEc2InstanceTypes(ec2Instances []*ec2.Instance) map[string]int {
	ec2InstanceTypes := make(map[string]int, 0)
	for _, instance := range ec2Instances {
		if *instance.State.Name != "terminated" && *instance.State.Name != "stopped" {
			ec2InstanceTypes[*instance.InstanceType]++
		}
	}
	return ec2InstanceTypes
}

// sumRdsInstanceTypes returns a count of RDS instances that AWS will charge for
func sumRdsInstanceTypes(rdsInstances []*rds.DBInstance) map[string]int {
	rdsInstanceTypes := make(map[string]int, 0)
	for _, instance := range rdsInstances {
		rdsInstanceTypes[*instance.DBInstanceClass]++
	}
	return rdsInstanceTypes
}
