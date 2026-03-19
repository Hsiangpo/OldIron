package firecrawlsvc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var urlWeights = map[string]int{
	"contact": 100,
	"support": 95,
	"help": 90,
	"customer": 85,
	"about": 80,
	"team": 78,
	"leadership": 76,
	"management": 74,
	"director": 72,
	"board": 70,
	"privacy": 68,
	"legal": 66,
	"imprint": 64,
	"career": 62,
	"job": 60,
	"press": 58,
	"media": 56,
	"terms": 54,
	"policy": 52,
	".pdf": 50,
}

type Service struct {
	config     Config
	httpClient *http.Client
	keyPool    *keyPool
}

func DefaultConfig() Config {
	return Config{
		BaseURL:        getenv("FIRECRAWL_BASE_URL", "https://api.firecrawl.dev/v2/"),
		TimeoutSeconds: getenvInt("FIRECRAWL_TIMEOUT_SECONDS", 45),
		MaxRetries:     getenvInt("FIRECRAWL_MAX_RETRIES", 2),
		KeyPerLimit:    getenvInt("FIRECRAWL_KEY_PER_LIMIT", 2),
		MapLimit:       200,
		PrefilterLimit: getenvInt("FIRECRAWL_PREFILTER_LIMIT", 40),
		ExtractMaxURLs: getenvInt("FIRECRAWL_EXTRACT_MAX_URLS", 12),
		ZeroRetryS:     getenvFloat("FIRECRAWL_ZERO_RETRY_SECONDS", 43200),
		ContactRetryS:  getenvFloat("FIRECRAWL_CONTACT_FORM_RETRY_SECONDS", 259200),
		ProxyURL:       getenv("HTTPS_PROXY", getenv("HTTP_PROXY", "http://127.0.0.1:7897")),
	}
}

func NewService(config Config) (*Service, error) {
	pool, err := newKeyPool(config.KeyPerLimit)
	if err != nil {
		return nil, err
	}
	return &Service{
		config:     config,
		httpClient: buildHTTPClient(config.ProxyURL, config.TimeoutSeconds),
		keyPool:    pool,
	}, nil
}

func (service *Service) DiscoverEmails(request DiscoverEmailsRequest) (DiscoverEmailsResponse, error) {
	startURL := normalizeStartURL(request.Homepage, request.Domain)
	if startURL == "" {
		return DiscoverEmailsResponse{}, nil
	}
	mappedURLs, err := service.mapSite(startURL)
	if err != nil {
		return DiscoverEmailsResponse{}, err
	}
	candidates := prefilterURLs(startURL, mappedURLs, service.config.PrefilterLimit)
	selected := buildFinalURLs(startURL, candidates, service.config.ExtractMaxURLs)
	result, err := service.extractEmails(selected)
	if err != nil {
		return DiscoverEmailsResponse{}, err
	}
	result.SelectedURLs = selected
	if len(result.Emails) == 0 {
		if result.ContactFormOnly {
			result.RetryAfterSeconds = service.config.ContactRetryS
		} else {
			result.RetryAfterSeconds = service.config.ZeroRetryS
		}
	}
	return result, nil
}

func (service *Service) MapSite(request MapSiteRequest) (MapSiteResponse, error) {
	startURL := normalizeStartURL(request.Homepage, request.Domain)
	if startURL == "" {
		return MapSiteResponse{}, nil
	}
	limit := request.Limit
	if limit <= 0 {
		limit = service.config.MapLimit
	}
	links, err := service.mapSiteWithOptions(startURL, limit, request.IncludeSubdomains)
	if err != nil {
		return MapSiteResponse{}, err
	}
	return MapSiteResponse{
		StartURL: startURL,
		Links:    links,
	}, nil
}

func (service *Service) ScrapeHTMLPages(request ScrapeHTMLPagesRequest) (ScrapeHTMLPagesResponse, error) {
	pages := make([]HTMLPage, 0, len(request.URLs))
	for _, rawURL := range request.URLs {
		targetURL := strings.TrimSpace(rawURL)
		if targetURL == "" {
			continue
		}
		html, err := service.scrapeHTML(targetURL)
		if err != nil {
			return ScrapeHTMLPagesResponse{}, err
		}
		if strings.TrimSpace(html) == "" {
			continue
		}
		pages = append(pages, HTMLPage{
			URL:  targetURL,
			HTML: html,
		})
	}
	return ScrapeHTMLPagesResponse{Pages: pages}, nil
}

func (service *Service) mapSite(startURL string) ([]string, error) {
	return service.mapSiteWithOptions(startURL, service.config.MapLimit, false)
}

func (service *Service) mapSiteWithOptions(startURL string, limit int, includeSubdomains bool) ([]string, error) {
	payload := map[string]any{
		"url":                   startURL,
		"limit":                 limit,
		"ignoreQueryParameters": true,
		"includeSubdomains":     includeSubdomains,
		"sitemap":               "include",
	}
	response, err := service.requestJSON(http.MethodPost, "map", payload)
	if err != nil {
		return nil, err
	}
	if links, ok := response["links"].([]any); ok {
		return anySliceToStrings(links), nil
	}
	if data, ok := response["data"].(map[string]any); ok {
		if links, ok := data["links"].([]any); ok {
			return anySliceToStrings(links), nil
		}
	}
	return nil, nil
}

func (service *Service) scrapeHTML(targetURL string) (string, error) {
	payload := map[string]any{
		"url":             targetURL,
		"formats":         []string{"rawHtml"},
		"onlyMainContent": false,
	}
	response, err := service.requestJSON(http.MethodPost, "scrape", payload)
	if err != nil {
		return "", err
	}
	return extractHTMLPayload(response), nil
}

func (service *Service) extractEmails(urls []string) (DiscoverEmailsResponse, error) {
	if len(urls) == 0 {
		return DiscoverEmailsResponse{}, nil
	}
	payload := map[string]any{
		"urls": urls,
		"prompt": "Extract publicly listed company email addresses from these official site pages. Return only real email addresses that appear on the pages or in mailto links. Prefer official company contact emails. Exclude fake placeholders. If the site only provides a contact form and no email, set contact_form_only=true and emails=[]. Always fill evidence_url and evidence_quote.",
		"schema": map[string]any{
			"type": "object",
			"properties": map[string]any{
				"emails":            map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
				"contact_form_only": map[string]any{"type": "boolean"},
				"evidence_url":      map[string]any{"type": "string"},
				"evidence_quote":    map[string]any{"type": "string"},
			},
			"required": []string{"emails", "contact_form_only", "evidence_url", "evidence_quote"},
		},
		"allowExternalLinks": false,
		"enableWebSearch":    false,
		"includeSubdomains":  false,
	}
	response, err := service.requestJSON(http.MethodPost, "extract", payload)
	if err != nil {
		return DiscoverEmailsResponse{}, err
	}
	if data, ok := response["data"].(map[string]any); ok && len(data) > 0 {
		return buildDiscoverResponse(extractJSONPayload(response)), nil
	}
	jobID, _ := response["id"].(string)
	if strings.TrimSpace(jobID) == "" {
		return DiscoverEmailsResponse{}, errors.New("firecrawl extract missing job id")
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(1500 * time.Millisecond)
		poll, pollErr := service.requestJSON(http.MethodGet, "extract/"+jobID, nil)
		if pollErr != nil {
			continue
		}
		status := strings.ToLower(strings.TrimSpace(toString(poll["status"])))
		if status == "completed" || status == "success" {
			return buildDiscoverResponse(extractJSONPayload(poll)), nil
		}
		if status == "failed" || status == "error" {
			return DiscoverEmailsResponse{}, errors.New("firecrawl extract failed")
		}
	}
	return DiscoverEmailsResponse{}, errors.New("firecrawl extract timeout")
}

func (service *Service) requestJSON(method string, path string, payload map[string]any) (map[string]any, error) {
	state, err := service.keyPool.acquire()
	if err != nil {
		return nil, err
	}
	defer service.keyPool.release(state)
	endpoint := strings.TrimRight(service.config.BaseURL, "/") + "/" + strings.TrimLeft(path, "/")
	var body io.Reader
	if payload != nil {
		bodyBytes, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return nil, marshalErr
		}
		body = bytes.NewReader(bodyBytes)
	}
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+state.value)
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := service.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusPaymentRequired {
		service.keyPool.disable(state)
		return nil, errors.New("firecrawl key unauthorized or out of credits")
	}
	if response.StatusCode >= 400 {
		return nil, errors.New("firecrawl request failed with status " + response.Status)
	}
	raw, err := readBodyString(response.Body)
	if err != nil {
		return nil, err
	}
	out := map[string]any{}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func normalizeStartURL(homepage string, domain string) string {
	if strings.HasPrefix(strings.TrimSpace(homepage), "http") {
		return strings.TrimSpace(homepage)
	}
	cleanDomain := strings.TrimSpace(domain)
	if cleanDomain == "" {
		return ""
	}
	return "https://" + cleanDomain
}

func prefilterURLs(startURL string, mappedURLs []string, limit int) []string {
	host := hostOnly(startURL)
	ranked := make([]string, 0, len(mappedURLs)+1)
	seen := map[string]bool{}
	for _, raw := range append([]string{startURL}, mappedURLs...) {
		value := strings.TrimSpace(raw)
		if value == "" || seen[value] || !strings.HasPrefix(value, "http") || !sameHost(host, value) {
			continue
		}
		seen[value] = true
		ranked = append(ranked, value)
	}
	sortByScore(startURL, ranked)
	if limit > 0 && len(ranked) > limit {
		return ranked[:limit]
	}
	return ranked
}

func buildFinalURLs(startURL string, candidates []string, limit int) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, raw := range append([]string{startURL}, candidates...) {
		value := strings.TrimSpace(raw)
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

func buildDiscoverResponse(payload map[string]any) DiscoverEmailsResponse {
	return DiscoverEmailsResponse{
		Emails:          normalizeEmails(payload["emails"]),
		EvidenceURL:     strings.TrimSpace(toString(payload["evidence_url"])),
		EvidenceQuote:   strings.TrimSpace(toString(payload["evidence_quote"])),
		ContactFormOnly: toBool(payload["contact_form_only"]),
	}
}

func extractJSONPayload(payload map[string]any) map[string]any {
	if data, ok := payload["data"].(map[string]any); ok {
		payload = data
	}
	for _, key := range []string{"data", "extract", "result"} {
		if child, ok := payload[key].(map[string]any); ok {
			return child
		}
	}
	return payload
}

func extractHTMLPayload(payload map[string]any) string {
	if data, ok := payload["data"].(map[string]any); ok {
		payload = data
	}
	for _, key := range []string{"rawHtml", "html"} {
		value := strings.TrimSpace(toString(payload[key]))
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeEmails(value any) []string {
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	out := []string{}
	for _, item := range list {
		text := strings.ToLower(strings.TrimSpace(toString(item)))
		if strings.Contains(text, "@") && !contains(out, text) {
			out = append(out, text)
		}
	}
	return out
}

func anySliceToStrings(values []any) []string {
	out := []string{}
	for _, item := range values {
		text := strings.TrimSpace(toString(item))
		if text != "" {
			out = append(out, text)
		}
	}
	return out
}

func hostOnly(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(parsed.Hostname())
}

func sameHost(host string, rawURL string) bool {
	target := hostOnly(rawURL)
	return target == host || strings.HasSuffix(target, "."+host) || strings.HasSuffix(host, "."+target)
}

func sortByScore(startURL string, values []string) {
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if urlScore(startURL, values[j]) > urlScore(startURL, values[i]) {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func urlScore(startURL string, rawURL string) int {
	if strings.TrimSpace(rawURL) == strings.TrimSpace(startURL) {
		return 1000
	}
	lower := strings.ToLower(rawURL)
	score := 0
	for keyword, weight := range urlWeights {
		if strings.Contains(lower, keyword) {
			score += weight
		}
	}
	score -= strings.Count(lower, "/")
	return score
}

func toString(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

func toBool(value any) bool {
	flag, ok := value.(bool)
	return ok && flag
}

func getenv(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value != "" {
		return value
	}
	return fallback
}

func getenvInt(name string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func getenvFloat(name string, fallback float64) float64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
