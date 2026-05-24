package agent

import (
	"strings"
	"testing"

	"go-rec/pkg/mq"
)

func TestPromptBuilderBuildCompactsSortsFiltersEscapesAndKeepsRecent(t *testing.T) {
	t.Parallel()

	events := []mq.Event{
		{UserID: "u1", ItemID: "old", CategoryID: "cat-old", Timestamp: 10, Action: "view"},
		{UserID: "", ItemID: "skip-user", CategoryID: "cat", Timestamp: 20, Action: "click"},
		{UserID: "u1", ItemID: "skip-item", CategoryID: "", Timestamp: 30, Action: "click"},
		{UserID: "u1", ItemID: "skip-time", CategoryID: "cat", Timestamp: 0, Action: "click"},
		{UserID: "u1", ItemID: "sku&2", CategoryID: "phone<flag>", Timestamp: 40, Action: "cart\"now\""},
		{UserID: "u1", ItemID: "sku'3", CategoryID: "case>hard", Timestamp: 50, Action: "buy'fast"},
		{UserID: "u1", ItemID: "sku4", CategoryID: "audio", Timestamp: 60, Action: "view"},
	}

	b := PromptBuilder{
		Template: PromptTemplate{
			SystemDirective:   "system & guard",
			ReasoningProtocol: "reason <only final>",
			OutputSchema:      `{"intent_vector":[0]}`,
		},
		MaxEvents: 2,
	}
	prompt, err := b.Build(&State{SessionID: "s1", BaselineVersion: 60, Events: events})
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	for _, tag := range []string{"system_directive", "behavior_log", "reasoning_protocol", "output_schema"} {
		if !strings.Contains(prompt, "<"+tag+">") || !strings.Contains(prompt, "</"+tag+">") {
			t.Fatalf("prompt missing XML tag %s: %s", tag, prompt)
		}
	}
	if strings.Contains(prompt, "old") || strings.Contains(prompt, "skip-") || strings.Contains(prompt, "sku&2") {
		t.Fatalf("prompt did not filter old/noise events correctly: %s", prompt)
	}
	casePos := strings.Index(prompt, "sku&#39;3")
	audioPos := strings.Index(prompt, "sku4")
	if casePos < 0 || audioPos < 0 || casePos > audioPos {
		t.Fatalf("prompt did not keep latest events in ascending time order: %s", prompt)
	}
	for _, escaped := range []string{"system &amp; guard", "reason &lt;only final&gt;", "sku&#39;3", "case&gt;hard", "buy&#39;fast"} {
		if !strings.Contains(prompt, escaped) {
			t.Fatalf("prompt missing escaped content %q: %s", escaped, prompt)
		}
	}
}

func TestPromptBuilderHandlesEmptyAndMaliciousInput(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("<script>&'\"", 2048)
	tests := []struct {
		name   string
		events []mq.Event
		want   string
	}{
		{name: "empty events still emits all XML sections", events: nil, want: "no_valid_events"},
		{name: "malicious long event is escaped", events: []mq.Event{{UserID: "u", ItemID: long, CategoryID: long, Timestamp: 1, Action: long}}, want: "&lt;script&gt;&amp;&#39;&quot;"},
	}

	builder := DefaultPromptBuilder()
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			prompt, err := builder.Build(&State{Events: tt.events})
			if err != nil {
				t.Fatalf("Build returned error: %v", err)
			}
			for _, tag := range []string{"system_directive", "behavior_log", "reasoning_protocol", "output_schema"} {
				if !strings.Contains(prompt, "<"+tag+">") || !strings.Contains(prompt, "</"+tag+">") {
					t.Fatalf("prompt missing XML tag %s: %s", tag, prompt)
				}
			}
			if !strings.Contains(prompt, tt.want) {
				t.Fatalf("prompt missing expected compact content %q", tt.want)
			}
			if strings.Contains(prompt, "<script>") {
				t.Fatalf("prompt leaked raw XML-like payload: %s", prompt)
			}
		})
	}
}

func TestCompactBehaviorLogFiltersSortsAndLimits(t *testing.T) {
	t.Parallel()

	log := CompactBehaviorLog([]mq.Event{
		{UserID: "u", ItemID: "late", CategoryID: "c2", Timestamp: 30, Action: "click"},
		{UserID: "u", ItemID: "early", CategoryID: "c1", Timestamp: 10, Action: "view"},
		{UserID: "u", ItemID: "", CategoryID: "bad", Timestamp: 20, Action: "skip"},
		{UserID: "u", ItemID: "mid", CategoryID: "c&", Timestamp: 20, Action: "cart"},
	}, 2)

	if strings.Contains(log, "early") || strings.Contains(log, "skip") {
		t.Fatalf("log did not retain only latest valid events: %s", log)
	}
	midPos := strings.Index(log, "mid")
	latePos := strings.Index(log, "late")
	if midPos < 0 || latePos < 0 || midPos > latePos {
		t.Fatalf("log not sorted ascending after limiting: %s", log)
	}
	if !strings.Contains(log, "c&amp;") {
		t.Fatalf("log did not XML escape category: %s", log)
	}
}
