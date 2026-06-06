package service

import (
	"testing"
)

func TestFormatEngineerMessage_Daily(t *testing.T) {
	msg := formatEngineerMessage("Alice", "daily", 80, 20.00, 25.00)
	want := "Heads up Alice — you've hit 80% of today's $25.00 Claude budget ($20.00 spent)."
	if msg != want {
		t.Errorf("\ngot:  %q\nwant: %q", msg, want)
	}
}

func TestFormatEngineerMessage_Monthly(t *testing.T) {
	msg := formatEngineerMessage("Bob", "monthly", 100, 500.00, 500.00)
	want := "Heads up Bob — you've hit 100% of this month's $500.00 Claude budget ($500.00 spent)."
	if msg != want {
		t.Errorf("\ngot:  %q\nwant: %q", msg, want)
	}
}

func TestFormatManagerMessage_Daily(t *testing.T) {
	msg := formatManagerMessage("Carol", "daily", 100, 25.00, 25.00)
	want := "Carol just hit 100% of their $25.00 daily Claude budget ($25.00 spent). Worth a check-in."
	if msg != want {
		t.Errorf("\ngot:  %q\nwant: %q", msg, want)
	}
}

func TestFormatManagerMessage_Monthly(t *testing.T) {
	msg := formatManagerMessage("Dave", "monthly", 80, 400.00, 500.00)
	want := "Dave just hit 80% of their $500.00 monthly Claude budget ($400.00 spent). Worth a check-in."
	if msg != want {
		t.Errorf("\ngot:  %q\nwant: %q", msg, want)
	}
}
