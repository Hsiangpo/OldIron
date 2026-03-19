package gmapsvc

import (
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"strings"
)

var phonePattern = regexp.MustCompile(`(?:\+\d{1,3}|0)[0-9\s\-]{7,}`)

func readBodyString(reader io.Reader) (string, error) {
	bytes, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func parseTBMMapPayload(text string) (any, error) {
	var outer any
	cleaned := strings.TrimSpace(stripXSSI(text))
	if cleaned == "" {
		return nil, errors.New("empty google maps response")
	}
	if err := json.Unmarshal([]byte(cleaned), &outer); err == nil {
		if payload := extractEmbeddedPayload(outer); payload != nil {
			return payload, nil
		}
		return outer, nil
	}
	index := strings.Index(cleaned, "[")
	if index < 0 {
		return nil, errors.New("google maps payload parse failed")
	}
	var fallback any
	if err := json.Unmarshal([]byte(cleaned[index:]), &fallback); err != nil {
		return nil, err
	}
	if payload := extractEmbeddedPayload(fallback); payload != nil {
		return payload, nil
	}
	return fallback, nil
}

func stripXSSI(text string) string {
	if strings.HasPrefix(text, ")]}'") {
		parts := strings.SplitN(text, "\n", 2)
		if len(parts) == 2 {
			return parts[1]
		}
		return ""
	}
	return text
}

func extractEmbeddedPayload(value any) any {
	list, ok := value.([]any)
	if !ok || len(list) == 0 {
		return value
	}
	first, ok := list[0].([]any)
	if ok && len(first) > 1 {
		text, ok := first[1].(string)
		if ok && strings.TrimSpace(text) != "" {
			var payload any
			if err := json.Unmarshal([]byte(stripXSSI(text)), &payload); err == nil {
				return payload
			}
		}
	}
	return value
}

func flattenStrings(value any) []string {
	out := []string{}
	var walk func(node any)
	walk = func(node any) {
		switch typed := node.(type) {
		case string:
			out = append(out, typed)
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(value)
	return out
}

func looksLikePlaceEntry(entry any) bool {
	values := flattenStrings(entry)
	hasPlaceID := false
	for _, item := range values {
		if strings.Contains(item, "0x") && strings.Contains(item, ":0x") {
			hasPlaceID = true
			break
		}
	}
	if !hasPlaceID {
		return false
	}
	return extractWebsite(entry) != "" || extractPhone(entry) != ""
}

func findPlaceEntries(payload any) []any {
	out := []any{}
	var walk func(node any)
	walk = func(node any) {
		list, ok := node.([]any)
		if !ok {
			return
		}
		if looksLikePlaceEntry(list) {
			out = append(out, list)
		}
		for _, child := range list {
			walk(child)
		}
	}
	walk(payload)
	return out
}

func extractWebsite(value any) string {
	for _, item := range flattenStrings(value) {
		text := strings.TrimSpace(item)
		if text == "" {
			continue
		}
		if strings.HasPrefix(text, "http://") || strings.HasPrefix(text, "https://") || strings.HasPrefix(text, "www.") {
			normalized := normalizeURL(text)
			if normalized != "" && !isBlockedHost(normalized) {
				return normalized
			}
		}
	}
	return ""
}

func extractPhone(value any) string {
	for _, item := range flattenStrings(value) {
		text := normalizeText(item)
		if phonePattern.MatchString(text) {
			return text
		}
	}
	return ""
}

