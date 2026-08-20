package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type ProviderError struct {
	IsTechnical bool
	Message     string
}

func (e *ProviderError) Error() string {
	return e.Message
}

// ProviderRequest is the External DTO
type ProviderRequest struct {
	MerchantName       string `json:"merchant_name"`
	TaxIdentifier      string `json:"tax_identifier"`
	RegistrationNumber string `json:"registration_number"`
}

// ProviderResponse is the External DTO
type ProviderResponse struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
}

type ProviderStatusResponse struct {
	RequestID string `json:"requestId"`
	Status    string `json:"status"`
	Amount    int64  `json:"amount"`
}

type ProviderClient struct {
	httpClient *http.Client
	baseURL    string
}

func NewProviderClient() *ProviderClient {
	return &ProviderClient{
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Crucial: 10-second timeout
		},
		baseURL: "https://api.provider.com/merchant/onboard", // Mock URL
	}
}

func (c *ProviderClient) Submit(ctx context.Context, req *ProviderRequest) (*ProviderResponse, error) {
	body, _ := json.Marshal(req)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewBuffer(body))
	if err != nil {
		// Network/Context errors are technical
		return nil, &ProviderError{IsTechnical: true, Message: err.Error()}
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, &ProviderError{IsTechnical: true, Message: err.Error()}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return nil, &ProviderError{IsTechnical: true, Message: fmt.Sprintf("Provider technical failure: %d", resp.StatusCode)}
	}
	if resp.StatusCode >= 400 {
		return nil, &ProviderError{IsTechnical: false, Message: fmt.Sprintf("Provider business failure: %d", resp.StatusCode)}
	}

	var providerResp ProviderResponse
	if err := json.NewDecoder(resp.Body).Decode(&providerResp); err != nil {
		return nil, &ProviderError{IsTechnical: true, Message: "Failed to parse provider response"}
	}

	return &providerResp, nil
}

// GetStatus checks the actual status of the transaction on the external provider's end
func (c *ProviderClient) GetStatus(ctx context.Context, extReqID string) (*ProviderStatusResponse, error) {
	url := c.baseURL + "/status/" + extReqID

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, errors.New("provider status API error")
	}

	var statusResp ProviderStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statusResp); err != nil {
		return nil, err
	}

	return &statusResp, nil
}