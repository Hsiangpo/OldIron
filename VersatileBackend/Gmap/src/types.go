package gmapsvc

type SearchCompanyProfileRequest struct {
	Query       string `json:"query"`
	CompanyName string `json:"company_name"`
}

type SearchCompanyProfileResponse struct {
	CompanyName string `json:"company_name"`
	Phone       string `json:"phone"`
	Website     string `json:"website"`
	Score       int    `json:"score"`
}

type Config struct {
	HL       string
	GL       string
	BaseURL  string
	PB       string
	ProxyURL string
	TimeoutS int
}

type candidate struct {
	Name      string
	Website   string
	Phone     string
	Score     int
	LocalName string
}

