package main

import (
	"io"
	"log"
	"sync"
)

// NewMetrics returns a new Metrics. The first arguments should be an
// io.writer that will send the metrics, and the bufferSize is the amount of metrics
// to keep in memory until they have been written
func NewMetrics(writer io.Writer, bufferSize int) *Metrics {
	return &Metrics{
		client: writer,
		size:   bufferSize,
	}
}

// Metrics keeps a circular buffer of metric to send.
type Metrics struct {
	MetricsSent int
	client      io.Writer
	size        int
	bufferLock  sync.Mutex
	buffer      []string
}

// Add adds a new metric to the buffer. It returns false if oldest
// metrics had to be removed to fit the new metric in the buffer
// Note: that the metrics should not be ending with a newline
func (m *Metrics) Add(s string) bool {

	m.bufferLock.Lock()
	defer m.bufferLock.Unlock()

	m.buffer = append(m.buffer, s)
	if len(m.buffer) > m.size {
		log.Printf("Buffer overflow")
		m.buffer = append(m.buffer[:0], m.buffer[1:]...)
		return false
	}
	return true
}

// Write will send the whole buffer and then clean it up
func (m *Metrics) Send() (int, error) {

	if len(m.buffer) == 0 {
		return 0, nil
	}

	var data []byte
	var idx int

	m.bufferLock.Lock()
	for idx = range m.buffer {
		data = append(data, []byte(m.buffer[idx]+"\n")...)
	}
	m.buffer = m.buffer[0:idx] // delete from buffer
	m.bufferLock.Unlock()
	m.MetricsSent += idx + 1
	return m.client.Write(data)
}

/*

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


// assumeRole uses the STS get assume the roleARN role and returns a Session that
// can by used by service clients
func assumeRole(roleARN, region string) *session.Session {
	return session.New(&aws.Config{
		Credentials: stscreds.NewCredentials(session.New(&aws.Config{}), roleARN),
		Region:      aws.String(region),
	})


*/
