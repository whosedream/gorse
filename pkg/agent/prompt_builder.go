package agent

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"go-rec/pkg/mq"
)

const (
	MetadataPreviousIntentText = "previous_intent_text"
	MetadataReflectionActive   = "reflection_active"

	maxPreviousIntentRunes = 200
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
	reflection, reflected := buildReflectionInstruction(st)
	var sb strings.Builder
	sb.Grow(len(b.Template.SystemDirective) + len(b.Template.ReasoningProtocol) + len(b.Template.OutputSchema) + len(behavior) + len(reflection) + 192)
	writeXMLSection(&sb, "system_directive", b.Template.SystemDirective)
	writeXMLRawSection(&sb, "behavior_log", behavior)
	if reflected {
		writeXMLSection(&sb, "reflection_instruction", reflection)
	}
	writeXMLSection(&sb, "reasoning_protocol", b.Template.ReasoningProtocol)
	writeXMLSection(&sb, "output_schema", b.Template.OutputSchema)
	return sb.String(), nil
}

func buildReflectionInstruction(st *State) (string, bool) {
	if st == nil || len(st.Metadata) == 0 {
		return "", false
	}
	previous := st.Metadata[MetadataPreviousIntentText]
	if previous == "" {
		return "", false
	}
	clicks := latestClickEvents(st.Events)
	if len(clicks) == 0 {
		return "", false
	}
	primaryCategory := clicks[len(clicks)-1].CategoryID
	if primaryCategory == "" || previousIntentAffirmsCategory(previous, primaryCategory) {
		return "", false
	}
	st.Metadata[MetadataReflectionActive] = "true"
	previous = truncateRunes(previous, maxPreviousIntentRunes)

	var sb strings.Builder
	sb.Grow(len(previous) + len(clicks)*64 + 192)
	sb.WriteString("【系统反思指令】\n")
	sb.WriteString("旧意图摘要: ")
	sb.WriteString(previous)
	sb.WriteString("\n最新连续点击: ")
	for i, ev := range clicks {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString("item=")
		sb.WriteString(ev.ItemID)
		sb.WriteString(" category=")
		sb.WriteString(ev.CategoryID)
	}
	sb.WriteString("\n最终 JSON 结构、字段名、字段类型必须与 output_schema 100% 一致。")
	sb.WriteString("\n仅输出最终的 JSON 字符串，严禁包含任何解释性文本、道歉用语或自然语言回复。")
	return sb.String(), true
}

func previousIntentAffirmsCategory(previous string, category string) bool {
	normalizedPrevious := normalizeIntentText(previous)
	normalizedCategory := normalizeIntentText(category)
	if normalizedCategory == "" || !strings.Contains(normalizedPrevious, normalizedCategory) {
		return false
	}
	for _, marker := range []string{"不再关注", "忽略", "摒弃", "不是", "非", "不"} {
		normalizedMarker := normalizeIntentText(marker)
		if strings.Contains(normalizedPrevious, normalizedMarker+normalizedCategory) || strings.Contains(normalizedPrevious, normalizedCategory+normalizedMarker) {
			return false
		}
	}
	return true
}

func normalizeIntentText(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '：' || r == ':' || r == '，' || r == ',' || r == '；' || r == ';' || r == '。' || r == '.' {
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

func latestClickEvents(events []mq.Event) []mq.Event {
	if len(events) == 0 {
		return nil
	}
	clicks := make([]mq.Event, 0, len(events))
	for _, ev := range events {
		if ev.Action == "click" && ev.CategoryID != "" {
			clicks = append(clicks, ev)
		}
	}
	if len(clicks) == 0 {
		return nil
	}
	sort.SliceStable(clicks, func(i, j int) bool { return clicks[i].Timestamp < clicks[j].Timestamp })
	return clicks
}

func truncateRunes(s string, max int) string {
	if max <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max])
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
