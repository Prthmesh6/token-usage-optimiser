package proxy

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"sync"
)

// StreamInterceptor passes SSE chunks through to the client while capturing
// assistant text from OpenAI-compatible streaming responses.
type StreamInterceptor struct {
	http.ResponseWriter

	mu          sync.Mutex
	captured    strings.Builder
	pending     []byte
	statusCode  int
	wroteHeader bool
}

// NewStreamInterceptor wraps w for streaming capture.
func NewStreamInterceptor(w http.ResponseWriter) *StreamInterceptor {
	return &StreamInterceptor{ResponseWriter: w, statusCode: http.StatusOK}
}

// StatusCode returns the HTTP status passed to WriteHeader.
func (s *StreamInterceptor) StatusCode() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.statusCode
}

// CapturedText returns the stitched assistant response captured from the stream.
func (s *StreamInterceptor) CapturedText() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captured.String()
}

// WriteHeader records the status and forwards it downstream.
func (s *StreamInterceptor) WriteHeader(statusCode int) {
	s.mu.Lock()
	s.statusCode = statusCode
	s.wroteHeader = true
	s.mu.Unlock()
	s.ResponseWriter.WriteHeader(statusCode)
}

// Flush forwards flush to the underlying writer when supported.
func (s *StreamInterceptor) Flush() {
	if flusher, ok := s.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Write streams bytes to the client immediately and parses SSE payloads for capture.
func (s *StreamInterceptor) Write(b []byte) (int, error) {
	log.Printf("[Interceptor Raw Write]: %s", string(b))

	n, err := s.ResponseWriter.Write(b)
	if err != nil {
		return n, err
	}
	if n > 0 {
		s.ingest(b[:n])
	}

	log.Printf("[Interceptor Accumulated Text]: %s", s.CapturedText())
	return n, nil
}

func (s *StreamInterceptor) ingest(chunk []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.pending = append(s.pending, chunk...)
	for {
		idx := bytes.IndexByte(s.pending, '\n')
		if idx < 0 {
			break
		}

		line := strings.TrimSpace(string(s.pending[:idx]))
		s.pending = s.pending[idx+1:]
		s.processLine(line)
	}

	if len(s.pending) > 0 && s.pending[0] == '{' && json.Valid(s.pending) {
		s.processLine(string(s.pending))
		s.pending = s.pending[:0]
	}
}

func (s *StreamInterceptor) processLine(line string) {
	if line == "" {
		return
	}

	if strings.HasPrefix(line, "data: ") {
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
		if payload == "" || payload == "[DONE]" {
			return
		}
		s.appendStreamDelta(payload)
		return
	}

	trimmed := strings.TrimSpace(line)
	if strings.HasPrefix(trimmed, "{") {
		s.appendCompletionJSON(trimmed)
	}
}

type streamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

type completionChunk struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

func (s *StreamInterceptor) appendStreamDelta(payload string) {
	var chunk streamChunk
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		log.Printf("[Interceptor JSON Error]: %v for chunk %s", err, payload)
		return
	}
	if len(chunk.Choices) == 0 {
		return
	}
	s.captured.WriteString(chunk.Choices[0].Delta.Content)
}

func (s *StreamInterceptor) appendCompletionJSON(payload string) {
	var chunk completionChunk
	if err := json.Unmarshal([]byte(payload), &chunk); err != nil {
		log.Printf("[Interceptor JSON Error]: %v for chunk %s", err, payload)
		return
	}
	if len(chunk.Choices) == 0 {
		return
	}
	content := chunk.Choices[0].Message.Content
	if content == "" {
		content = chunk.Choices[0].Delta.Content
	}
	if content != "" {
		s.captured.WriteString(content)
	}
}

// Unwrap exposes the underlying ResponseWriter for http.ResponseController.
func (s *StreamInterceptor) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

var _ http.Flusher = (*StreamInterceptor)(nil)
