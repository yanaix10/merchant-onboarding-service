package handlers

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"time"

	"merchant-onboarding-service/internal/repository"

	"github.com/gin-gonic/gin"
)

type ProviderWebhookRequest struct {
	EventID   string    `json:"eventId"`
	RequestID string    `json:"requestId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type WebhookHandler struct {
	repo   *repository.MerchantRepository
	secret string
}

func NewWebhookHandler(repo *repository.MerchantRepository) *WebhookHandler {
	// In production, fetch from Vault/Env
	secret := os.Getenv("WEBHOOK_SECRET_KEY")
	if secret == "" {
		secret = "FintechWebhookSecret2026"
	}
	return &WebhookHandler{repo: repo, secret: secret}
}

func (h *WebhookHandler) HandleProviderCallback(c *gin.Context) {
	// 1. Read Raw Payload for Validation
	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read body"})
		return
	}

	// 2. Validate HMAC Signature
	signature := c.GetHeader("X-Signature")
	if !h.validateSignature(rawBody, signature) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid signature"})
		return
	}

	// 3. Deserialize JSON
	var payload ProviderWebhookRequest
	if err := json.Unmarshal(rawBody, &payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid payload format"})
		return
	}

	// 4. Idempotency & Audit Check
	ctx := c.Request.Context()
	err = h.repo.SaveWebhookAudit(ctx, payload.EventID, string(rawBody))
	if err != nil {
		// Duplicate event (Constraint Violation), return 200 OK immediately
		c.JSON(http.StatusOK, gin.H{"message": "Already processed"})
		return
	}

	// 5. Locate Transaction
	tx, err := h.repo.FindByExternalRequestID(ctx, payload.RequestID)
	if err != nil {
		// Return 200 OK to prevent provider from retrying indefinitely for a missing record
		c.JSON(http.StatusOK, gin.H{"error": "Transaction not found"})
		return
	}

	// 6. Check Business Idempotency (Final State Check)
	if tx.Status == repository.StatusCompleted || tx.Status == repository.StatusFailed {
		c.JSON(http.StatusOK, gin.H{"message": "Transaction already in final state"})
		return
	}

	// 7. Map Status and Update
	mappedStatus := h.mapProviderStatus(payload.Status)
	var completedAt *time.Time
	if mappedStatus == repository.StatusCompleted || mappedStatus == repository.StatusFailed {
		now := time.Now()
		completedAt = &now
	}

	if err := h.repo.UpdateWebhookStatus(ctx, tx.TrackingID, mappedStatus, completedAt); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	// 8. Acknowledge Provider
	c.JSON(http.StatusOK, gin.H{"message": "Webhook processed successfully"})
}

func (h *WebhookHandler) validateSignature(payload []byte, signature string) bool {
	if signature == "" {
		return false
	}
	mac := hmac.New(sha256.New, []byte(h.secret))
	mac.Write(payload)
	expectedMAC := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expectedMAC), []byte(signature))
}

func (h *WebhookHandler) mapProviderStatus(providerStatus string) repository.TransactionStatus {
	switch providerStatus {
	case "APPROVED":
		return repository.StatusCompleted
	case "REJECTED":
		return repository.StatusFailed
	default:
		return repository.StatusProcessing
	}
}