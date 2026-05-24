package agent

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"go-rec/pkg/mq"
)

var errNilState = errors.New("agent nil state")

// PromptTemplate holds the XML sections injected around compact behavior logs.
type PromptTemplate struct {
	SystemDirective   string
	ReasoningProtocol string
	OutputSchema      string
}

// PromptBuilder compresses raw behavior events into a deterministic prompt.
type PromptBuilder struct {
	Template  PromptTemplate
	MaxEvents int
}

// DefaultPromptBuilder returns the slow-track intent extraction prompt template.
func DefaultPromptBuilder() PromptBuilder {
	return PromptBuilder{
		Template: PromptTemplate{
			SystemDirective:   "You are an ecommerce intent extraction node. Return final JSON only.",
			ReasoningProtocol: "Use behavior chronology to infer category weights and a 1024-dimensional intent_vector. Do not expose hidden reasoning.",
			OutputSchema:      `{"session_id":"string","baseline_version":0,"category_weights":{"category":0.0},"intent_vector":[0.0]}`,
		},
		MaxEvents: 64,
	}
}

// Build renders the full XML prompt with escaped dynamic content.
func (b PromptBuilder) Build(st *State) (string, error) {
	if st == nil {
		return "", errNilState
	}
	if b.Template.SystemDirective == "" && b.Template.ReasoningProtocol == "" && b.Template.OutputSchema == "" {
		b = DefaultPromptBuilder()
	}
	behavior := CompactBehaviorLog(st.Events, b.MaxEvents)
	var sb strings.Builder
	sb.Grow(len(b.Template.SystemDirective) + len(b.Template.ReasoningProtocol) + len(b.Template.OutputSchema) + len(behavior) + 160)
	writeXMLSection(&sb, "system_directive", b.Template.SystemDirective)
	writeXMLRawSection(&sb, "behavior_log", behavior)
	writeXMLSection(&sb, "reasoning_protocol", b.Template.ReasoningProtocol)
	writeXMLSection(&sb, "output_schema", b.Template.OutputSchema)
	return sb.String(), nil
}

// CompactBehaviorLog filters noise, sorts by timestamp, and keeps the latest max events.
func CompactBehaviorLog(events []mq.Event, max int) string {
	valid := make([]mq.Event, 0, len(events))
	for _, ev := range events {
		if ev.UserID == "" || ev.ItemID == "" || ev.CategoryID == "" || ev.Timestamp <= 0 {
			continue
		}
		valid = append(valid, ev)
	}
	if len(valid) == 0 {
		return "no_valid_events"
	}
	sort.SliceStable(valid, func(i, j int) bool { return valid[i].Timestamp < valid[j].Timestamp })
	if max > 0 && len(valid) > max {
		valid = valid[len(valid)-max:]
	}
	var sb strings.Builder
	sb.Grow(len(valid) * 64)
	for i, ev := range valid {
		if i > 0 {
			sb.WriteByte('\n')
		}
		sb.WriteString("t=")
		sb.WriteString(strconv.FormatInt(ev.Timestamp, 10))
		sb.WriteString(" user=")
		writeEscaped(&sb, ev.UserID)
		sb.WriteString(" item=")
		writeEscaped(&sb, ev.ItemID)
		sb.WriteString(" category=")
		writeEscaped(&sb, ev.CategoryID)
		sb.WriteString(" action=")
		writeEscaped(&sb, ev.Action)
	}
	return sb.String()
}

func writeXMLSection(sb *strings.Builder, tag string, value string) {
	sb.WriteByte('<')
	sb.WriteString(tag)
	sb.WriteString(">\n")
	writeEscaped(sb, value)
	sb.WriteString("\n</")
	sb.WriteString(tag)
	sb.WriteString(">\n")
}

func writeXMLRawSection(sb *strings.Builder, tag string, value string) {
	sb.WriteByte('<')
	sb.WriteString(tag)
	sb.WriteString(">\n")
	sb.WriteString(value)
	sb.WriteString("\n</")
	sb.WriteString(tag)
	sb.WriteString(">\n")
}

func writeEscaped(sb *strings.Builder, s string) {
	for _, r := range s {
		switch r {
		case '&':
			sb.WriteString("&amp;")
		case '<':
			sb.WriteString("&lt;")
		case '>':
			sb.WriteString("&gt;")
		case '"':
			sb.WriteString("&quot;")
		case '\'':
			sb.WriteString("&#39;")
		default:
			sb.WriteRune(r)
		}
	}
}
