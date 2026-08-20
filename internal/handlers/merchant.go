package handlers

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"merchant-onboarding-service/internal/client"
	"merchant-onboarding-service/internal/events"
	"merchant-onboarding-service/internal/repository"
	"merchant-onboarding-service/internal/saga"
	"merchant-onboarding-service/internal/statemachine"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type OnboardRequest struct {
	MerchantName string `json:"merchantName" binding:"required"`
	PanNumber    string `json:"panNumber" binding:"required"`
	GstNumber    string `json:"gstNumber" binding:"required"`
}

type OnboardResponse struct {
	TrackingID string `json:"trackingId"`
	Status     string `json:"status"`
}

type MerchantHandler struct {
	repo         *repository.MerchantRepository
	idempotency  *repository.IdempotencyRepository
	client       *client.ProviderClient
	stateMachine *statemachine.StateMachine
	orchestrator *saga.OnboardingSagaOrchestrator // Added the Orchestrator
}

func NewMerchantHandler(
	repo *repository.MerchantRepository,
	idempotency *repository.IdempotencyRepository,
	client *client.ProviderClient,
	stateMachine *statemachine.StateMachine,
	orchestrator *saga.OnboardingSagaOrchestrator, // Injected the Orchestrator
) *MerchantHandler {
	return &MerchantHandler{
		repo:         repo,
		idempotency:  idempotency,
		client:       client,
		stateMachine: stateMachine,
		orchestrator: orchestrator,
	}
}

func (h *MerchantHandler) Onboard(c *gin.Context) {
	// 1. Extract Idempotency Key
	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}

	// 2. Read Raw Body and Generate SHA-256 Hash
	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}
	requestHash := fmt.Sprintf("%x", sha256.Sum256(rawBody))

	// 3. Check for Existing Idempotency Record
	existingRecord, _ := h.idempotency.FindByKey(c.Request.Context(), idempotencyKey)
	if existingRecord != nil {
		// Hash Mismatch Check
		if existingRecord.RequestHash != requestHash {
			c.JSON(http.StatusConflict, gin.H{"error": "Idempotency key reused with different payload"})
			return
		}

		// Return exactly what was processed last time
		var savedResponse OnboardResponse
		json.Unmarshal([]byte(existingRecord.ResponsePayload), &savedResponse)
		c.JSON(http.StatusAccepted, savedResponse)
		return
	}

	// 4. Deserialize Request
	var req OnboardRequest
	if err := json.Unmarshal(rawBody, &req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid JSON format"})
		return
	}

	trackingID := "TRK-" + uuid.New().String()

	// 5. Save Initial State (RECEIVED) + Generate Outbox Event
	entity := &repository.MerchantEntity{
		TrackingID:     trackingID,
		Status:         repository.StatusReceived,
		RequestPayload: string(rawBody),
		RetryCount:     0,
		CreatedAt:      time.Now(),
		UpdatedAt:      time.Now(),
	}

	eventID := "EVT-" + uuid.New().String()
	createdEvent := events.TransactionCreatedEvent{
		TrackingID: trackingID,
		Status:     string(repository.StatusReceived),
		OccurredAt: time.Now(),
	}
	eventBytes, _ := json.Marshal(createdEvent)

	// Atomic Save: Business Data + Outbox Event
	if err := h.repo.Save(c.Request.Context(), entity, eventID, "TransactionCreatedEvent", string(eventBytes)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to initialize transaction"})
		return
	}

	// ---------------------------------------------------------
	// 6. Start the Distributed Saga Orchestrator Workflow
	// ---------------------------------------------------------
	merchantData := map[string]interface{}{
		"merchantName": req.MerchantName,
		"panNumber":    req.PanNumber,
		"gstNumber":    req.GstNumber,
	}

	if err := h.orchestrator.Start(c.Request.Context(), trackingID, merchantData); err != nil {
		// We just log the error. The system will recover this saga via a background scheduler later.
		log.Printf("Failed to start saga for %s: %v", trackingID, err)
	}
	// ---------------------------------------------------------

	// 7. Build Final Response (Return ACCEPTED_FOR_PROCESSING)
	// The client gets a sub-10ms response while the Saga does the heavy lifting via Kafka!
	finalResponse := OnboardResponse{
		TrackingID: trackingID,
		Status:     "ACCEPTED_FOR_PROCESSING",
	}
	finalResponseBytes, _ := json.Marshal(finalResponse)

	// 8. Save Idempotency Record
	idempEntity := &repository.IdempotencyEntity{
		IdempotencyKey:  idempotencyKey,
		TrackingID:      trackingID,
		RequestHash:     requestHash,
		ResponsePayload: string(finalResponseBytes),
	}
	if err := h.idempotency.Save(c.Request.Context(), idempEntity); err != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "Concurrent request processing detected"})
		return
	}

	c.JSON(http.StatusAccepted, finalResponse)
}

func (h *MerchantHandler) GetStatus(c *gin.Context) {
	trackingID := c.Param("trackingId")

	entity, err := h.repo.FindByTrackingID(c.Request.Context(), trackingID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Transaction not found"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"trackingId": entity.TrackingID,
		"status":     entity.Status,
	})
}	