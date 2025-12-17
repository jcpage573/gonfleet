package destination

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/onfleet/gonfleet"
	"github.com/onfleet/gonfleet/testingutil"
)

func TestClient_Get(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	mockClient.AddResponse("/destinations/destination_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	destination, err := client.Get("destination_123")

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)
	assert.Equal(t, expectedDestination.Address.Street, destination.Address.Street)

	mockClient.AssertRequestMade("GET", "/destinations/destination_123")
	mockClient.AssertBasicAuth("test_api_key")
}

func TestClient_Create(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	mockClient.AddResponse("/destinations", testingutil.MockResponse{
		StatusCode: 201,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	params := onfleet.DestinationCreateParams{
		Address: onfleet.DestinationAddress{
			Number:     "456",
			Street:     "Test Street",
			City:       "Test City",
			State:      "CA",
			PostalCode: "12345",
			Country:    "US",
		},
		Notes: "Test destination",
	}

	destination, err := client.Create(params)

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)

	mockClient.AssertRequestMade("POST", "/destinations")
}

func TestClient_ListWithMetadataQuery(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestinations := []onfleet.Destination{
		testingutil.GetSampleDestination(),
	}

	mockClient.AddResponse("/destinations/metadata", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestinations,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "location_type",
			Type:  "string",
			Value: "warehouse",
		},
	}

	destinations, err := client.ListWithMetadataQuery(metadata)

	assert.NoError(t, err)
	assert.Len(t, destinations, 1)
	assert.Equal(t, expectedDestinations[0].ID, destinations[0].ID)

	mockClient.AssertRequestMade("POST", "/destinations/metadata")
}

func TestClient_ErrorScenarios(t *testing.T) {
	tests := []struct {
		name       string
		method     string
		url        string
		statusCode int
	}{
		{"get not found", "GET", "/destinations/nonexistent", 404},
		{"create invalid", "POST", "/destinations", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			mockClient.AddResponse(tt.url, testingutil.MockResponse{
				StatusCode: tt.statusCode,
				Body:       testingutil.GetSampleErrorResponse(),
			})

			client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

			var err error
			switch tt.method {
			case "GET":
				_, err = client.Get("nonexistent")
			case "POST":
				_, err = client.Create(onfleet.DestinationCreateParams{})
			}

			assert.Error(t, err)
		})
	}
}

func TestClient_MetadataSet(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	expectedDestination.Metadata = []onfleet.Metadata{
		{
			Name:  "location_type",
			Type:  "string",
			Value: "warehouse",
		},
	}

	mockClient.AddResponse("/destinations/destination_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "location_type",
			Type:  "string",
			Value: "warehouse",
		},
	}

	destination, err := client.MetadataSet("destination_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)

	// Verify the field was set
	assert.Len(t, destination.Metadata, 1)
	assert.Equal(t, "location_type", destination.Metadata[0].Name)
	assert.Equal(t, "warehouse", destination.Metadata[0].Value)

	mockClient.AssertRequestMade("PUT", "/destinations/destination_123")
}

func TestClient_MetadataSet_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	expectedDestination.Metadata = []onfleet.Metadata{
		{
			Name:  "building_code",
			Type:  "string",
			Value: "B123",
		},
		{
			Name:  "floor",
			Type:  "number",
			Value: float64(3),
		},
	}

	mockClient.AddResponse("/destinations/destination_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	// Set only floor field
	metadata := []onfleet.Metadata{
		{
			Name:  "floor",
			Type:  "number",
			Value: float64(3),
		},
	}

	destination, err := client.MetadataSet("destination_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)

	// Verify both fields are present (atomicity - building_code was preserved)
	assert.Len(t, destination.Metadata, 2)

	var foundBuilding, foundFloor bool
	for _, m := range destination.Metadata {
		if m.Name == "building_code" {
			foundBuilding = true
			assert.Equal(t, "B123", m.Value)
		}
		if m.Name == "floor" {
			foundFloor = true
			assert.Equal(t, float64(3), m.Value)
		}
	}
	assert.True(t, foundBuilding, "building_code field should be preserved")
	assert.True(t, foundFloor, "floor field should be set")

	mockClient.AssertRequestMade("PUT", "/destinations/destination_123")
}

func TestClient_MetadataPop(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	expectedDestination.Metadata = []onfleet.Metadata{}

	mockClient.AddResponse("/destinations/destination_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	destination, err := client.MetadataPop("destination_123", "temp_flag")

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)

	// Verify the field was removed
	assert.Len(t, destination.Metadata, 0)
	for _, m := range destination.Metadata {
		assert.NotEqual(t, "temp_flag", m.Name, "temp_flag field should be removed")
	}

	mockClient.AssertRequestMade("PUT", "/destinations/destination_123")
}

func TestClient_MetadataPop_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedDestination := testingutil.GetSampleDestination()
	expectedDestination.Metadata = []onfleet.Metadata{
		{
			Name:  "location_type",
			Type:  "string",
			Value: "residential",
		},
	}

	mockClient.AddResponse("/destinations/destination_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedDestination,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/destinations", mockClient.MockCaller)

	destination, err := client.MetadataPop("destination_123", "old_field")

	assert.NoError(t, err)
	assert.Equal(t, expectedDestination.ID, destination.ID)

	// Verify location_type was preserved (atomicity)
	assert.Len(t, destination.Metadata, 1)
	assert.Equal(t, "location_type", destination.Metadata[0].Name)
	assert.Equal(t, "residential", destination.Metadata[0].Value)

	// Verify old_field is not present
	for _, m := range destination.Metadata {
		assert.NotEqual(t, "old_field", m.Name, "old_field should not be present")
	}

	mockClient.AssertRequestMade("PUT", "/destinations/destination_123")
}