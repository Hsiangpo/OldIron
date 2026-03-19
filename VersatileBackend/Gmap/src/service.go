package gmapsvc

import (
	"errors"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSearchPB = "!1z6aaZ5riv6aSQ5Y6F!4m8!1m3!1d29520.55694754669!2d114.125!3d22.351!3m2!1i1024!2i768!4f13.1!7i20!10b1!12m50!1m5!18b1!30b1!31m1!1b1!34e1!2m4!5m1!6e2!20e3!39b1!6m22!49b1!63m0!66b1!74i150000!85b1!91b1!114b1!149b1!206b1!212b1!213b1!223b1!227b1!232b1!233b1!239b1!244b1!246b1!250b1!253b1!258b1!263b1!10b1!12b1!13b1!14b1!16b1!17m1!3e1!20m4!5e2!6b1!8b1!14b1!46m1!1b0!96b1!99b1!19m4!2m3!1i360!2i120!4i8!20m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m33!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!1m3!1e9!2b1!3e2!2b1!9b0!15m8!1m7!1m2!1m1!1e2!2m2!1i195!2i195!3i20!22m5!1s_udLaffGBaTl2roP3dzD4A0!7e81!14m1!3s_udLaffGBaTl2roP3dzD4A0!15i9937!24m107!1m28!13m9!2b1!3b1!4b1!6i1!8b1!9b1!14b1!20b1!25b1!18m17!3b1!4b1!5b1!6b1!13b1!14b1!17b1!21b1!22b1!27m1!1b0!28b0!32b1!33m1!1b1!34b1!36e2!10m1!8e3!11m1!3e1!14m1!3b0!17b1!20m2!1e3!1e6!24b1!25b1!26b1!27b1!29b1!30m1!2b1!36b1!37b1!39m3!2m2!2i1!3i1!43b1!52b1!54m1!1b1!55b1!56m1!1b1!61m2!1m1!1e1!65m5!3m4!1m3!1m2!1i224!2i298!72m22!1m8!2b1!5b1!7b1!12m4!1b1!2b1!4m1!1e1!4b1!8m10!1m6!4m1!1e1!4m1!1e3!4m1!1e4!3sother_user_google_review_posts__and__hotel_and_vr_partner_review_posts!6m1!1e1!9b1!89b1!98m3!1b1!2b1!3b1!103b1!113b1!114m3!1b1!2m1!1b1!117b1!122m1!1b1!126b1!127b1!26m4!2m3!1i80!2i92!4i8!30m28!1m6!1m2!1i0!2i0!2m2!1i530!2i768!1m6!1m2!1i974!2i0!2m2!1i1024!2i768!1m6!1m2!1i0!2i0!2m2!1i1024!2i20!1m6!1m2!1i0!2i748!2m2!1i1024!2i768!34m19!2b1!3b1!4b1!6b1!8m6!1b1!3b1!4b1!5b1!6b1!7b1!9b1!12b1!14b1!20b1!23b1!25b1!26b1!31b1!37m1!1e81!42b1!49m10!3b1!6m2!1b1!2b1!7m2!1e3!2b1!8b1!9b1!10e2!50m3!2e2!3m1!3b1!61b1!67m5!7b1!10b1!14b1!15m1!1b0!69i761"

func DefaultConfig() Config {
	return Config{
		HL:       getenv("GMAP_HL", "en"),
		GL:       getenv("GMAP_GL", "dk"),
		BaseURL:  getenv("GMAP_BASE_URL", "https://www.google.com"),
		PB:       getenv("GMAP_SEARCH_PB", defaultSearchPB),
		ProxyURL: getenv("GOOGLE_MAPS_PROXY_URL", "http://127.0.0.1:7897"),
		TimeoutS: getenvInt("GMAP_TIMEOUT_SECONDS", 30),
	}
}

type Service struct {
	config     Config
	httpClient *http.Client
}

func NewService(config Config) *Service {
	return &Service{
		config:     config,
		httpClient: buildHTTPClient(config.ProxyURL, config.TimeoutS),
	}
}

func (service *Service) SearchCompanyProfile(query string, companyName string) (SearchCompanyProfileResponse, error) {
	normalizedQuery := normalizeText(query)
	if normalizedQuery == "" {
		return SearchCompanyProfileResponse{}, errors.New("empty query")
	}
	text, err := service.searchRaw(normalizedQuery)
	if err != nil {
		return SearchCompanyProfileResponse{}, err
	}
	payload, err := parseTBMMapPayload(text)
	if err != nil {
		return SearchCompanyProfileResponse{}, err
	}
	candidates := extractPlaceCandidates(payload, companyNameOrQuery(companyName, normalizedQuery))
	best := pickBestCandidate(candidates, companyNameOrQuery(companyName, normalizedQuery))
	if best == nil {
		return SearchCompanyProfileResponse{}, nil
	}
	return SearchCompanyProfileResponse{
		CompanyName: best.Name,
		Phone:       best.Phone,
		Website:     best.Website,
		Score:       best.Score,
	}, nil
}

func (service *Service) searchRaw(query string) (string, error) {
	params := url.Values{}
	params.Set("tbm", "map")
	params.Set("hl", service.config.HL)
	params.Set("gl", service.config.GL)
	params.Set("q", query)
	params.Set("pb", service.config.PB)
	endpoint := strings.TrimRight(service.config.BaseURL, "/") + "/search?" + params.Encode()
	request, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	request.Header.Set("Accept", "*/*")
	request.Header.Set("Accept-Language", "en-GB,en;q=0.9,en-US;q=0.8")
	request.Header.Set("Referer", "https://www.google.com/maps?hl=en&gl=dk")
	response, err := service.httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode >= 400 {
		return "", errors.New("google maps request failed with status " + response.Status)
	}
	body, err := readBodyString(response.Body)
	if err != nil {
		return "", err
	}
	return body, nil
}

func companyNameOrQuery(companyName string, query string) string {
	if strings.TrimSpace(companyName) != "" {
		return companyName
	}
	return query
}

func buildHTTPClient(proxyURL string, timeoutS int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
	}
	if strings.TrimSpace(proxyURL) != "" {
		if parsed, err := url.Parse(proxyURL); err == nil {
			transport.Proxy = http.ProxyURL(parsed)
		}
	}
	return &http.Client{
		Timeout:   time.Duration(maxInt(timeoutS, 5)) * time.Second,
		Transport: transport,
	}
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

func maxInt(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}
