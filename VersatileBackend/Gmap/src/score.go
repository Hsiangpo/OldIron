package gmapsvc

import (
	"net/url"
	"regexp"
	"strings"
)

var nonAlphaNumeric = regexp.MustCompile(`[^0-9a-z]+`)

var blockedHostHints = []string{
	"google.",
	"gstatic.",
	"googleusercontent.",
	"googleapis.",
	"g.page",
	"goo.gl",
	"facebook.com",
	"instagram.com",
	"twitter.com",
	"x.com",
	"linkedin.com",
	"youtube.com",
	"tiktok.com",
	"wikipedia.org",
	"wikidata.org",
	"wikimedia.org",
}

func extractPlaceCandidates(payload any, queryName string) []candidate {
	results := []candidate{}
	for _, entry := range findPlaceEntries(payload) {
		candidate := candidate{
			Name:    extractCandidateName(entry, queryName),
			Website: extractWebsite(entry),
			Phone:   extractPhone(entry),
		}
		candidate.Score = candidateScore(queryName, candidate)
		if candidate.Name != "" || candidate.Website != "" || candidate.Phone != "" {
			results = append(results, candidate)
		}
	}
	return results
}

func pickBestCandidate(candidates []candidate, queryName string) *candidate {
	var best *candidate
	for index := range candidates {
		score := candidateScore(queryName, candidates[index])
		candidates[index].Score = score
		if score < 45 {
			continue
		}
		if best == nil || score > best.Score {
			best = &candidates[index]
		}
	}
	return best
}

func candidateScore(queryName string, value candidate) int {
	base := maxInt(nameMatchScore(queryName, value.Name), domainMatchScore(queryName, value.Website))
	return base
}

func extractCandidateName(entry any, queryName string) string {
	bestName := ""
	bestScore := -1
	for _, item := range flattenStrings(entry) {
		text := normalizeText(item)
		if text == "" || strings.HasPrefix(text, "http://") || strings.HasPrefix(text, "https://") || strings.HasPrefix(text, "www.") {
			continue
		}
		score := nameMatchScore(queryName, text)
		if score > bestScore {
			bestScore = score
			bestName = text
		}
	}
	return bestName
}

func nameMatchScore(queryName string, candidateName string) int {
	query := normalizeNameForMatch(queryName)
	candidate := normalizeNameForMatch(candidateName)
	if query == "" || candidate == "" {
		return 0
	}
	if query == candidate {
		return 100
	}
	if strings.Contains(candidate, query) || strings.Contains(query, candidate) {
		return 70
	}
	if len(query) >= 4 && strings.Contains(candidate, query[:4]) {
		return 45
	}
	return 0
}

func domainMatchScore(queryName string, rawURL string) int {
	normalized := normalizeURL(rawURL)
	if normalized == "" {
		return 0
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return 0
	}
	host := strings.TrimPrefix(strings.ToLower(parsed.Hostname()), "www.")
	if host == "" {
		return 0
	}
	label := strings.Split(host, ".")[0]
	compact := normalizeNameForMatch(queryName)
	if compact == label || label == compact {
		return 100
	}
	if strings.HasPrefix(compact, label) || strings.HasPrefix(label, compact) {
		return 80
	}
	return 0
}

func normalizeNameForMatch(text string) string {
	value := strings.ToLower(normalizeText(text))
	return nonAlphaNumeric.ReplaceAllString(value, "")
}

func normalizeText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func normalizeURL(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}
	if strings.HasPrefix(text, "www.") {
		text = "https://" + text
	}
	if !strings.HasPrefix(text, "http://") && !strings.HasPrefix(text, "https://") {
		return ""
	}
	parsed, err := url.Parse(text)
	if err != nil || parsed.Host == "" {
		return ""
	}
	parsed.Fragment = ""
	if parsed.Scheme == "http" {
		parsed.Scheme = "https"
	}
	return strings.TrimRight(parsed.String(), "/")
}

func isBlockedHost(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return true
	}
	host := strings.ToLower(parsed.Hostname())
	for _, hint := range blockedHostHints {
		if strings.Contains(host, hint) {
			return true
		}
	}
	return false
}
