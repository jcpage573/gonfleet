package recipient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/onfleet/gonfleet"
	"github.com/onfleet/gonfleet/testingutil"
)

func TestClient_Get(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	recipient, err := client.Get("recipient_123")

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)
	assert.Equal(t, expectedRecipient.Name, recipient.Name)
	assert.Equal(t, expectedRecipient.Phone, recipient.Phone)

	mockClient.AssertRequestMade("GET", "/recipients/recipient_123")
	mockClient.AssertBasicAuth("test_api_key")
}

func TestClient_FindByName(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	mockClient.AddResponse("recipients/name", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	recipient, err := client.Find("Jane Smith", onfleet.RecipientQueryKeyName)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)
	assert.Equal(t, expectedRecipient.Name, recipient.Name)

	mockClient.AssertRequestMade("GET", "/recipients/name/")
}

func TestClient_FindByPhone(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	mockClient.AddResponse("/recipients/phone/+15559876543", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	recipient, err := client.Find("+15559876543", onfleet.RecipientQueryKeyPhone)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)
	assert.Equal(t, expectedRecipient.Phone, recipient.Phone)

	mockClient.AssertRequestMade("GET", "/recipients/phone/+15559876543")
}

func TestClient_Create(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	mockClient.AddResponse("/recipients", testingutil.MockResponse{
		StatusCode: 201,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	params := onfleet.RecipientCreateParams{
		Name:  "Bob Johnson",
		Phone: "+15551112222",
		Notes: "Preferred contact time: evenings",
	}

	recipient, err := client.Create(params)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)

	mockClient.AssertRequestMade("POST", "/recipients")
}

func TestClient_Update(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	expectedRecipient.Notes = "Updated notes"

	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	params := onfleet.RecipientUpdateParams{
		Notes: "Updated notes",
	}

	recipient, err := client.Update("recipient_123", params)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)
	assert.Equal(t, "Updated notes", recipient.Notes)

	mockClient.AssertRequestMade("PUT", "/recipients/recipient_123")
}

func TestClient_ListWithMetadataQuery(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipients := []onfleet.Recipient{
		testingutil.GetSampleRecipient(),
	}

	mockClient.AddResponse("/recipients/metadata", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipients,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "customer_type",
			Type:  "string",
			Value: "premium",
		},
	}

	recipients, err := client.ListWithMetadataQuery(metadata)

	assert.NoError(t, err)
	assert.Len(t, recipients, 1)
	assert.Equal(t, expectedRecipients[0].ID, recipients[0].ID)

	mockClient.AssertRequestMade("POST", "/recipients/metadata")
}

func TestClient_ErrorScenarios(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		url        string
		statusCode int
		operation  func(client *Client) error
	}{
		{
			name:       "get not found",
			method:     "GET",
			url:        "/recipients/nonexistent",
			statusCode: 404,
			operation: func(client *Client) error {
				_, err := client.Get("nonexistent")
				return err
			},
		},
		{
			name:       "find by name not found",
			method:     "GET",
			url:        "/recipients/name/Unknown",
			statusCode: 404,
			operation: func(client *Client) error {
				_, err := client.Find("Unknown", onfleet.RecipientQueryKeyName)
				return err
			},
		},
		{
			name:       "find by phone not found",
			method:     "GET",
			url:        "/recipients/phone/+15550000000",
			statusCode: 404,
			operation: func(client *Client) error {
				_, err := client.Find("+15550000000", onfleet.RecipientQueryKeyPhone)
				return err
			},
		},
		{
			name:       "create invalid",
			method:     "POST",
			url:        "/recipients",
			statusCode: 400,
			operation: func(client *Client) error {
				_, err := client.Create(onfleet.RecipientCreateParams{})
				return err
			},
		},
		{
			name:       "update not found",
			method:     "PUT",
			url:        "/recipients/nonexistent",
			statusCode: 404,
			operation: func(client *Client) error {
				_, err := client.Update("nonexistent", onfleet.RecipientUpdateParams{})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			mockClient.AddResponse(tt.url, testingutil.MockResponse{
				StatusCode: tt.statusCode,
				Body:       testingutil.GetSampleErrorResponse(),
			})

			client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

			err := tt.operation(client)
			assert.Error(t, err)
		})
	}
}

func TestClient_PhoneNumberEncoding(t *testing.T) {
	tests := []struct {
		name         string
		phoneNumber  string
		expectedURL  string
	}{
		{
			name:         "US phone number with plus",
			phoneNumber:  "+15551234567",
			expectedURL:  "/recipients/phone/+15551234567",
		},
		{
			name:         "international phone",
			phoneNumber:  "+442071234567",
			expectedURL:  "/recipients/phone/+442071234567",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedRecipient := testingutil.GetSampleRecipient()
			expectedRecipient.Phone = tt.phoneNumber

			mockClient.AddResponse(tt.expectedURL, testingutil.MockResponse{
				StatusCode: 200,
				Body:       expectedRecipient,
			})

			client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

			recipient, err := client.Find(tt.phoneNumber, onfleet.RecipientQueryKeyPhone)

			assert.NoError(t, err)
			assert.Equal(t, tt.phoneNumber, recipient.Phone)
		})
	}
}

func TestClient_MetadataSet(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	expectedRecipient.Metadata = []onfleet.Metadata{
		{
			Name:  "customer_id",
			Type:  "string",
			Value: "CUST12345",
		},
	}

	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "customer_id",
			Type:  "string",
			Value: "CUST12345",
		},
	}

	recipient, err := client.MetadataSet("recipient_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)

	// Verify the field was set
	assert.Len(t, recipient.Metadata, 1)
	assert.Equal(t, "customer_id", recipient.Metadata[0].Name)
	assert.Equal(t, "CUST12345", recipient.Metadata[0].Value)

	mockClient.AssertRequestMade("PUT", "/recipients/recipient_123")
}

func TestClient_MetadataSet_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	expectedRecipient.Metadata = []onfleet.Metadata{
		{
			Name:  "customer_tier",
			Type:  "string",
			Value: "gold",
		},
		{
			Name:  "loyalty_points",
			Type:  "number",
			Value: float64(500),
		},
	}

	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	// Set only loyalty_points field
	metadata := []onfleet.Metadata{
		{
			Name:  "loyalty_points",
			Type:  "number",
			Value: float64(500),
		},
	}

	recipient, err := client.MetadataSet("recipient_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)

	// Verify both fields are present (atomicity - customer_tier was preserved)
	assert.Len(t, recipient.Metadata, 2)

	var foundTier, foundPoints bool
	for _, m := range recipient.Metadata {
		if m.Name == "customer_tier" {
			foundTier = true
			assert.Equal(t, "gold", m.Value)
		}
		if m.Name == "loyalty_points" {
			foundPoints = true
			assert.Equal(t, float64(500), m.Value)
		}
	}
	assert.True(t, foundTier, "customer_tier field should be preserved")
	assert.True(t, foundPoints, "loyalty_points field should be set")

	mockClient.AssertRequestMade("PUT", "/recipients/recipient_123")
}

func TestClient_MetadataPop(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	expectedRecipient.Metadata = []onfleet.Metadata{}

	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	recipient, err := client.MetadataPop("recipient_123", "temp_note")

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)

	// Verify the field was removed
	assert.Len(t, recipient.Metadata, 0)
	for _, m := range recipient.Metadata {
		assert.NotEqual(t, "temp_note", m.Name, "temp_note field should be removed")
	}

	mockClient.AssertRequestMade("PUT", "/recipients/recipient_123")
}

func TestClient_MetadataPop_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedRecipient := testingutil.GetSampleRecipient()
	expectedRecipient.Metadata = []onfleet.Metadata{
		{
			Name:  "customer_id",
			Type:  "string",
			Value: "CUST99999",
		},
	}

	mockClient.AddResponse("/recipients/recipient_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedRecipient,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/recipients", mockClient.MockCaller)

	recipient, err := client.MetadataPop("recipient_123", "old_field")

	assert.NoError(t, err)
	assert.Equal(t, expectedRecipient.ID, recipient.ID)

	// Verify customer_id was preserved (atomicity)
	assert.Len(t, recipient.Metadata, 1)
	assert.Equal(t, "customer_id", recipient.Metadata[0].Name)
	assert.Equal(t, "CUST99999", recipient.Metadata[0].Value)

	// Verify old_field is not present
	for _, m := range recipient.Metadata {
		assert.NotEqual(t, "old_field", m.Name, "old_field should not be present")
	}

	mockClient.AssertRequestMade("PUT", "/recipients/recipient_123")
}