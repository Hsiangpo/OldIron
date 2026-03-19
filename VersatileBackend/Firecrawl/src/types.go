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

