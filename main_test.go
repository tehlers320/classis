package main

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"
)

type MockWriter struct {
	Content []string
}

func (w *MockWriter) Write(p []byte) (n int, err error) {
	w.Content = make([]string, 0)
	scanner := bufio.NewScanner(bytes.NewBuffer(p))
	for scanner.Scan() {
		w.Content = append(w.Content, scanner.Text())
	}
	return len(p), nil
}

func TestSimpleWrite(t *testing.T) {
	mock := &MockWriter{}
	metrics := NewMetrics(mock, 2)

	actual := []string{
		"namespace.metric 1 11111",
	}

	metrics.Add(actual[0])
	metrics.Send()
	if !reflect.DeepEqual(actual, mock.Content) {
		t.Errorf("Expected MockWriter.Content to be equal to '%s', got '%s'.", actual, mock.Content)
	}
}

func TestMultipleMetrics(t *testing.T) {
	mock := &MockWriter{}
	metrics := NewMetrics(mock, 2)

	actual := []string{
		"namespace.metric 1 11111",
		"namespace.metric 2 22222",
	}

	metrics.Add(actual[0])
	metrics.Add(actual[1])
	metrics.Send()

	if !reflect.DeepEqual(actual, mock.Content) {
		t.Errorf("Expected MockWriter.Content to be equal to '%s', got '%s'.", actual, mock.Content)
	}
}

func TestAddPumpAddPump(t *testing.T) {
	mock := &MockWriter{}
	metrics := NewMetrics(mock, 2)

	actual := []string{
		"namespace.metric 1 11111",
		"namespace.metric 2 22222",
	}

	metrics.Add(actual[0])
	metrics.Send()

	metrics.Add(actual[1])
	metrics.Send()

	if len(mock.Content) != 1 {
		t.Errorf("Expected that there would be one written metric in the MockWriter, got %d", len(mock.Content))
	}

	if mock.Content[0] != actual[1] {
		t.Errorf("Expected MockWriter.Content to be equal to '%s', got '%s'.", actual[1], mock.Content[0])
	}
}

func TestDropOldMetrics(t *testing.T) {
	mock := &MockWriter{}
	metrics := NewMetrics(mock, 1)

	metrics.Add("namespace.metric 1 11111")
	second := "namespace.metric 2 22222"
	metrics.Add(second)
	metrics.Send()

	if mock.Content[0] != second {
		t.Errorf("Expected MockWriter.Content[0] to be equal to '%s', got '%s'.", second, mock.Content[0])
	}
}
