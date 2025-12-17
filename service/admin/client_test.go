package admin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/onfleet/gonfleet"
	"github.com/onfleet/gonfleet/testingutil"
)

func TestClient_List(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmins := []onfleet.Admin{
		testingutil.GetSampleAdmin(),
	}

	mockClient.AddResponse("/admins", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmins,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	admins, err := client.List()

	assert.NoError(t, err)
	assert.Len(t, admins, 1)
	assert.Equal(t, expectedAdmins[0].ID, admins[0].ID)
	assert.Equal(t, expectedAdmins[0].Email, admins[0].Email)
	assert.Equal(t, expectedAdmins[0].Name, admins[0].Name)

	mockClient.AssertRequestMade("GET", "/admins")
	mockClient.AssertBasicAuth("test_api_key")
}

func TestClient_Create(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	mockClient.AddResponse("/admins", testingutil.MockResponse{
		StatusCode: 201,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	params := onfleet.AdminCreateParams{
		Email:      "newadmin@example.com",
		Name:       "Jane Admin",
		Phone:      "+15559876543",
		IsReadOnly: false,
		Type:       "standard",
		Metadata: []onfleet.Metadata{
			{
				Name:  "department",
				Type:  "string",
				Value: "logistics",
			},
		},
	}

	admin, err := client.Create(params)

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)
	assert.Equal(t, expectedAdmin.Email, admin.Email)

	mockClient.AssertRequestMade("POST", "/admins")
}

func TestClient_Update(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	expectedAdmin.Name = "Updated Admin Name"
	expectedAdmin.Phone = "+15550000000"

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	params := onfleet.AdminUpdateParams{
		Name:  "Updated Admin Name",
		Phone: "+15550000000",
		Metadata: []onfleet.Metadata{
			{
				Name:  "department",
				Type:  "string",
				Value: "updated_department",
			},
		},
	}

	admin, err := client.Update("admin_123", params)

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)
	assert.Equal(t, "Updated Admin Name", admin.Name)
	assert.Equal(t, "+15550000000", admin.Phone)

	mockClient.AssertRequestMade("PUT", "/admins/admin_123")
}

func TestClient_Delete(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       map[string]interface{}{"success": true},
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	err := client.Delete("admin_123")

	assert.NoError(t, err)
	mockClient.AssertRequestMade("DELETE", "/admins/admin_123")
}

func TestClient_ListWithMetadataQuery(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmins := []onfleet.Admin{
		testingutil.GetSampleAdmin(),
	}

	mockClient.AddResponse("/admins/metadata", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmins,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "department",
			Type:  "string",
			Value: "operations",
		},
	}

	admins, err := client.ListWithMetadataQuery(metadata)

	assert.NoError(t, err)
	assert.Len(t, admins, 1)
	assert.Equal(t, expectedAdmins[0].ID, admins[0].ID)

	mockClient.AssertRequestMade("POST", "/admins/metadata")
}

func TestClient_AdminTypes(t *testing.T) {
	tests := []struct {
		name       string
		adminType  string
		isReadOnly bool
	}{
		{
			name:       "standard admin",
			adminType:  "standard",
			isReadOnly: false,
		},
		{
			name:       "readonly admin",
			adminType:  "standard",
			isReadOnly: true,
		},
		{
			name:       "super admin",
			adminType:  "super",
			isReadOnly: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedAdmin := testingutil.GetSampleAdmin()
			expectedAdmin.Type = tt.adminType
			expectedAdmin.IsReadOnly = tt.isReadOnly

			mockClient.AddResponse("/admins", testingutil.MockResponse{
				StatusCode: 201,
				Body:       expectedAdmin,
			})

			client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

			params := onfleet.AdminCreateParams{
				Email:      "test@example.com",
				Name:       "Test Admin",
				Type:       tt.adminType,
				IsReadOnly: tt.isReadOnly,
			}

			admin, err := client.Create(params)

			assert.NoError(t, err)
			assert.Equal(t, tt.adminType, admin.Type)
			assert.Equal(t, tt.isReadOnly, admin.IsReadOnly)
		})
	}
}

func TestClient_AdminPermissions(t *testing.T) {
	tests := []struct {
		name           string
		isAccountOwner bool
		isActive       bool
		isReadOnly     bool
		teams          []string
	}{
		{
			name:           "account owner",
			isAccountOwner: true,
			isActive:       true,
			isReadOnly:     false,
			teams:          []string{},
		},
		{
			name:           "team admin with multiple teams",
			isAccountOwner: false,
			isActive:       true,
			isReadOnly:     false,
			teams:          []string{"team_123", "team_456", "team_789"},
		},
		{
			name:           "inactive admin",
			isAccountOwner: false,
			isActive:       false,
			isReadOnly:     true,
			teams:          []string{"team_123"},
		},
		{
			name:           "readonly admin",
			isAccountOwner: false,
			isActive:       true,
			isReadOnly:     true,
			teams:          []string{"team_123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedAdmin := testingutil.GetSampleAdmin()
			expectedAdmin.IsAccountOwner = tt.isAccountOwner
			expectedAdmin.IsActive = tt.isActive
			expectedAdmin.IsReadOnly = tt.isReadOnly
			expectedAdmin.Teams = tt.teams

			mockClient.AddResponse("/admins", testingutil.MockResponse{
				StatusCode: 200,
				Body:       []onfleet.Admin{expectedAdmin},
			})

			client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

			admins, err := client.List()

			assert.NoError(t, err)
			assert.Len(t, admins, 1)
			admin := admins[0]
			assert.Equal(t, tt.isAccountOwner, admin.IsAccountOwner)
			assert.Equal(t, tt.isActive, admin.IsActive)
			assert.Equal(t, tt.isReadOnly, admin.IsReadOnly)
			assert.Len(t, admin.Teams, len(tt.teams))
		})
	}
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
			name:       "create invalid email",
			method:     "POST",
			url:        "/admins",
			statusCode: 400,
			operation: func(client *Client) error {
				_, err := client.Create(onfleet.AdminCreateParams{
					Email: "invalid-email",
					Name:  "Test Admin",
				})
				return err
			},
		},
		{
			name:       "create duplicate email",
			method:     "POST",
			url:        "/admins",
			statusCode: 409,
			operation: func(client *Client) error {
				_, err := client.Create(onfleet.AdminCreateParams{
					Email: "existing@example.com",
					Name:  "Test Admin",
				})
				return err
			},
		},
		{
			name:       "update not found",
			method:     "PUT",
			url:        "/admins/nonexistent",
			statusCode: 404,
			operation: func(client *Client) error {
				_, err := client.Update("nonexistent", onfleet.AdminUpdateParams{
					Name: "Updated Name",
				})
				return err
			},
		},
		{
			name:       "delete account owner",
			method:     "DELETE",
			url:        "/admins/owner_123",
			statusCode: 403,
			operation: func(client *Client) error {
				return client.Delete("owner_123")
			},
		},
		{
			name:       "list unauthorized",
			method:     "GET",
			url:        "/admins",
			statusCode: 401,
			operation: func(client *Client) error {
				_, err := client.List()
				return err
			},
		},
		{
			name:       "metadata query invalid",
			method:     "POST",
			url:        "/admins/metadata",
			statusCode: 400,
			operation: func(client *Client) error {
				_, err := client.ListWithMetadataQuery([]onfleet.Metadata{
					{
						Name:  "invalid_field",
						Type:  "unknown",
						Value: "test",
					},
				})
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

			client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

			err := tt.operation(client)
			assert.Error(t, err)
		})
	}
}

func TestClient_MetadataSet(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	expectedAdmin.Metadata = []onfleet.Metadata{
		{
			Name:  "role",
			Type:  "string",
			Value: "manager",
		},
	}

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "role",
			Type:  "string",
			Value: "manager",
		},
	}

	admin, err := client.MetadataSet("admin_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)

	// Verify the field was set
	assert.Len(t, admin.Metadata, 1)
	assert.Equal(t, "role", admin.Metadata[0].Name)
	assert.Equal(t, "manager", admin.Metadata[0].Value)

	mockClient.AssertRequestMade("PUT", "/admins/admin_123")
}

func TestClient_MetadataSet_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	expectedAdmin.Metadata = []onfleet.Metadata{
		{
			Name:  "department",
			Type:  "string",
			Value: "operations",
		},
		{
			Name:  "level",
			Type:  "number",
			Value: float64(3),
		},
	}

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	// Set only level field
	metadata := []onfleet.Metadata{
		{
			Name:  "level",
			Type:  "number",
			Value: float64(3),
		},
	}

	admin, err := client.MetadataSet("admin_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)

	// Verify both fields are present (atomicity - department was preserved)
	assert.Len(t, admin.Metadata, 2)

	var foundDepartment, foundLevel bool
	for _, m := range admin.Metadata {
		if m.Name == "department" {
			foundDepartment = true
			assert.Equal(t, "operations", m.Value)
		}
		if m.Name == "level" {
			foundLevel = true
			assert.Equal(t, float64(3), m.Value)
		}
	}
	assert.True(t, foundDepartment, "department field should be preserved")
	assert.True(t, foundLevel, "level field should be set")

	mockClient.AssertRequestMade("PUT", "/admins/admin_123")
}

func TestClient_MetadataPop(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	expectedAdmin.Metadata = []onfleet.Metadata{}

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	admin, err := client.MetadataPop("admin_123", "temp_access")

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)

	// Verify the field was removed
	assert.Len(t, admin.Metadata, 0)
	for _, m := range admin.Metadata {
		assert.NotEqual(t, "temp_access", m.Name, "temp_access field should be removed")
	}

	mockClient.AssertRequestMade("PUT", "/admins/admin_123")
}

func TestClient_MetadataPop_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedAdmin := testingutil.GetSampleAdmin()
	expectedAdmin.Metadata = []onfleet.Metadata{
		{
			Name:  "department",
			Type:  "string",
			Value: "operations",
		},
	}

	mockClient.AddResponse("/admins/admin_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedAdmin,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/admins", mockClient.MockCaller)

	admin, err := client.MetadataPop("admin_123", "old_field")

	assert.NoError(t, err)
	assert.Equal(t, expectedAdmin.ID, admin.ID)

	// Verify department was preserved (atomicity)
	assert.Len(t, admin.Metadata, 1)
	assert.Equal(t, "department", admin.Metadata[0].Name)
	assert.Equal(t, "operations", admin.Metadata[0].Value)

	// Verify old_field is not present
	for _, m := range admin.Metadata {
		assert.NotEqual(t, "old_field", m.Name, "old_field should not be present")
	}

	mockClient.AssertRequestMade("PUT", "/admins/admin_123")
}