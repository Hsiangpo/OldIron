package firecrawlsvc

import (
	"encoding/json"
	"net/http"
)

func RegisterRoutes(mux *http.ServeMux) {
	service, err := NewService(DefaultConfig())
	if err != nil {
		mux.HandleFunc("/v1/discover-emails", func(writer http.ResponseWriter, _ *http.Request) {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
		})
		return
	}
	mux.HandleFunc("/v1/discover-emails", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload DiscoverEmailsRequest
		if decodeErr := json.NewDecoder(request.Body).Decode(&payload); decodeErr != nil {
			http.Error(writer, "invalid json body", http.StatusBadRequest)
			return
		}
		result, callErr := service.DiscoverEmails(payload)
		if callErr != nil {
			http.Error(writer, callErr.Error(), http.StatusBadGateway)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(result)
	})
}
