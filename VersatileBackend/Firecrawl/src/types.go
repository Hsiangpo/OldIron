package firecrawlsvc

type DiscoverEmailsRequest struct {
	CompanyName string `json:"company_name"`
	Homepage    string `json:"homepage"`
	Domain      string `json:"domain"`
}

type DiscoverEmailsResponse struct {
	Emails            []string `json:"emails"`
	EvidenceURL       string   `json:"evidence_url"`
	EvidenceQuote     string   `json:"evidence_quote"`
	ContactFormOnly   bool     `json:"contact_form_only"`
	RetryAfterSeconds float64  `json:"retry_after_seconds"`
	SelectedURLs      []string `json:"selected_urls"`
}

type MapSiteRequest struct {
	Homepage          string `json:"homepage"`
	Domain            string `json:"domain"`
	Limit             int    `json:"limit"`
	IncludeSubdomains bool   `json:"include_subdomains"`
}

type MapSiteResponse struct {
	StartURL string   `json:"start_url"`
	Links    []string `json:"links"`
}

type HTMLPage struct {
	URL  string `json:"url"`
	HTML string `json:"html"`
}

type ScrapeHTMLPagesRequest struct {
	URLs []string `json:"urls"`
}

type ScrapeHTMLPagesResponse struct {
	Pages []HTMLPage `json:"pages"`
}

type Config struct {
	BaseURL         string
	TimeoutSeconds  int
	MaxRetries      int
	KeyPerLimit     int
	MapLimit        int
	PrefilterLimit  int
	ExtractMaxURLs  int
	ZeroRetryS      float64
	ContactRetryS   float64
	ProxyURL        string
}
