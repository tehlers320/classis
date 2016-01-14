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
	for i := range m.buffer {
		data = append(data, []byte(m.buffer[i]+"\n")...)
	}

	m.MetricsSent += len(m.buffer)
	m.clearBuffer()

	return m.client.Write(data)
}

// clear the buffer
func (m *Metrics) clearBuffer() {
	m.bufferLock.Lock()
	m.buffer = nil
	m.bufferLock.Unlock()
}
