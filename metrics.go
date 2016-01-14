package main

import (
	"io"
	"log"
)

// NewMetrics returns a new Metrics thats main purpose it to
// keep a circular buffer of metric to send. The first arguments should be an
// io.writer that will send the metrics, and the bufferSize is the amount of metrics
// to keep in memory until they have been written
func NewMetrics(writer io.Writer, bufferSize int) *Metrics {
	return &Metrics{
		Client: writer,
		size:   bufferSize,
	}
}

type Metrics struct {
	// @todo buffer needs to be protected by a mutex so that the Add(), Pump() and clearBuffer() doesn't conflict
	buffer []string
	size   int
	Client io.Writer
}

// Add adds a new metric to the buffer. It returns false if oldest
// metrics had to be removed to fit the new metric in the buffer
// Note: that the metrics should not be ending with a newline
func (m *Metrics) Add(s string) bool {
	m.buffer = append(m.buffer, s)
	if len(m.buffer) > m.size {
		log.Printf("Buffer overflow")
		m.buffer = append(m.buffer[:0], m.buffer[1:]...)
		return false
	}
	return true
}

// Pump will write the whole buffer and then clean it up
func (m *Metrics) Pump() (int, error) {
	if len(m.buffer) == 0 {
		return 0, nil
	}
	// @todo(only clear the successfully sent data
	defer m.clearBuffer()
	var data []byte
	for i := range m.buffer {
		data = append(data, []byte(m.buffer[i]+"\n")...)
	}
	return m.Client.Write(data)
}

func (m *Metrics) clearBuffer() {
	m.buffer = nil
}
