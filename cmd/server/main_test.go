package main

import (
	"os"
	"strings"
	"testing"
)

func TestEnvFiles(t *testing.T) {
	t.Parallel()

	gitignore, err := os.ReadFile("../../.gitignore")
	if err != nil {
		t.Fatalf("read .gitignore: %v", err)
	}
	if !hasExactLine(string(gitignore), ".env") {
		t.Fatalf(".gitignore must contain exact .env line, got:\n%s", gitignore)
	}
	if hasExactLine(string(gitignore), ".env.example") {
		t.Fatal(".gitignore must not ignore .env.example")
	}

	example, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}
	for _, want := range []string{
		"EMBEDDING_BASE_URL=https://api.siliconflow.cn/v1/embeddings",
		"EMBEDDING_API_KEY=your_api_key_here",
		"EMBEDDING_MODEL=BAAI/bge-m3",
	} {
		if !strings.Contains(string(example), want) {
			t.Fatalf(".env.example missing %q; content:\n%s", want, example)
		}
	}
	for _, legacy := range []string{"SILICONFLOW_API_KEY=", "sk-"} {
		if strings.Contains(string(example), legacy) {
			t.Fatalf(".env.example must not contain %q; content:\n%s", legacy, example)
		}
	}
}

func hasExactLine(s, want string) bool {
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}
