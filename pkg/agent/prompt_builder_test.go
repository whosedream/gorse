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

func TestPromptBuilderAddsReflectionInstructionOnIntentReversal(t *testing.T) {
	t.Parallel()

	previous := strings.Repeat("母婴", 100) + "尾巴"
	st := &State{
		SessionID: "s-reflect",
		Metadata: map[string]string{
			"previous_intent_text": previous,
		},
		Events: []mq.Event{
			{UserID: "u1", ItemID: "phone-1", CategoryID: "数码", Timestamp: 10, Action: "click"},
			{UserID: "u1", ItemID: "phone-2", CategoryID: "数码", Timestamp: 20, Action: "click"},
		},
	}

	prompt, err := DefaultPromptBuilder().Build(st)
	if err != nil {
		t.Fatalf("Build returned error: %v", err)
	}

	for _, want := range []string{
		"<reflection_instruction>",
		"【系统反思指令】",
		"最新连续点击",
		"item=phone-1 category=数码",
		"item=phone-2 category=数码",
		"最终 JSON 结构、字段名、字段类型必须与 output_schema 100% 一致。",
		"仅输出最终的 JSON 字符串，严禁包含任何解释性文本、道歉用语或自然语言回复。",
	} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("reflection prompt missing %q: %s", want, prompt)
		}
	}
	if strings.Contains(prompt, "尾巴") {
		t.Fatalf("reflection prompt did not truncate previous intent with rune boundary: %s", prompt)
	}
	if !strings.Contains(prompt, "母婴") {
		t.Fatalf("reflection prompt lost complete Chinese runes: %s", prompt)
	}
	if got := st.Metadata["reflection_active"]; got != "true" {
		t.Fatalf("reflection_active metadata = %q, want true", got)
	}
}

func TestPromptBuilderReflectsWhenPreviousIntentMentionsNewCategoryAsRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous string
	}{
		{name: "adjacent negation", previous: "旧意图：母婴；用户不再关注数码"},
		{name: "punctuated negation", previous: "旧意图：母婴；用户不再关注：数码"},
		{name: "spaced negation", previous: "旧意图：母婴；用户不再关注 数码"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			st := &State{
				SessionID: "s-reflect-negated",
				Metadata: map[string]string{
					MetadataPreviousIntentText: tt.previous,
				},
				Events: []mq.Event{
					{UserID: "u1", ItemID: "phone-9", CategoryID: "数码", Timestamp: 30, Action: "click"},
				},
			}
			prompt, err := DefaultPromptBuilder().Build(st)
			if err != nil {
				t.Fatalf("Build returned error: %v", err)
			}
			if !strings.Contains(prompt, "<reflection_instruction>") {
				t.Fatalf("prompt did not reflect after negated category mention: %s", prompt)
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
