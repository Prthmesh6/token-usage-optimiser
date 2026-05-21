package tokenizer

import (
	"fmt"
	"sync"

	"github.com/pkoukk/tiktoken-go"
)

const encodingName = "cl100k_base"

var (
	encOnce sync.Once
	enc     *tiktoken.Tiktoken
	encErr  error
)

func loadEncoding() {
	enc, encErr = tiktoken.GetEncoding(encodingName)
}

// EstimateTokens returns the token count for text using the cl100k_base encoding.
// The vocabulary is loaded exactly once on first use.
func EstimateTokens(text string) (int, error) {
	encOnce.Do(loadEncoding)
	if encErr != nil {
		return 0, fmt.Errorf("tokenizer: load encoding: %w", encErr)
	}
	tokens := enc.Encode(text, nil, nil)
	return len(tokens), nil
}
