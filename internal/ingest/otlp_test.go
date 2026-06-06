package ingest

import (
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestKeyValuesToMap(t *testing.T) {
	kvs := []*commonpb.KeyValue{
		{Key: "user.email", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "alice@co.com"}}},
		{Key: "model", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "claude-opus-4-8"}}},
	}
	m := keyValuesToMap(kvs)
	if got := m["user.email"]; got != "alice@co.com" {
		t.Errorf("user.email = %q, want %q", got, "alice@co.com")
	}
	if got := m["model"]; got != "claude-opus-4-8" {
		t.Errorf("model = %q, want %q", got, "claude-opus-4-8")
	}
	if len(m) != 2 {
		t.Errorf("len = %d, want 2", len(m))
	}
}

func TestKeyValuesToMap_Empty(t *testing.T) {
	m := keyValuesToMap(nil)
	if len(m) != 0 {
		t.Errorf("expected empty map, got len %d", len(m))
	}
}

func TestAnyValueString(t *testing.T) {
	cases := []struct {
		name string
		v    *commonpb.AnyValue
		want string
	}{
		{"nil", nil, ""},
		{"string", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}}, "hello"},
		{"bool_true", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"bool_false", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}, "false"},
		{"int", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 42}}, "42"},
		{"double", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14}}, "3.14"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := anyValueString(c.v)
			if got != c.want {
				t.Errorf("anyValueString(%v) = %q, want %q", c.v, got, c.want)
			}
		})
	}
}

func TestNumberValue(t *testing.T) {
	dpDouble := &metricspb.NumberDataPoint{
		Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.5},
	}
	dpInt := &metricspb.NumberDataPoint{
		Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7},
	}
	dpNil := &metricspb.NumberDataPoint{}

	if got := numberValue(dpDouble); got != 1.5 {
		t.Errorf("AsDouble: got %v, want 1.5", got)
	}
	if got := numberValue(dpInt); got != 7.0 {
		t.Errorf("AsInt: got %v, want 7.0", got)
	}
	if got := numberValue(dpNil); got != 0 {
		t.Errorf("nil value: got %v, want 0", got)
	}
}

func TestExtractNumberDataPoints_Sum(t *testing.T) {
	dp := &metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.05}}
	m := &metricspb.Metric{
		Data: &metricspb.Metric_Sum{
			Sum: &metricspb.Sum{DataPoints: []*metricspb.NumberDataPoint{dp}},
		},
	}
	pts := extractNumberDataPoints(m)
	if len(pts) != 1 {
		t.Fatalf("len = %d, want 1", len(pts))
	}
	if pts[0] != dp {
		t.Error("wrong data point returned")
	}
}

func TestExtractNumberDataPoints_Gauge(t *testing.T) {
	dp := &metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsInt{AsInt: 3}}
	m := &metricspb.Metric{
		Data: &metricspb.Metric_Gauge{
			Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{dp}},
		},
	}
	pts := extractNumberDataPoints(m)
	if len(pts) != 1 {
		t.Fatalf("len = %d, want 1", len(pts))
	}
}

func TestExtractNumberDataPoints_Unknown(t *testing.T) {
	m := &metricspb.Metric{} // no data set
	pts := extractNumberDataPoints(m)
	if pts != nil {
		t.Errorf("expected nil for unknown metric type, got %v", pts)
	}
}
