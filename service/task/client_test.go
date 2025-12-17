package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/onfleet/gonfleet"
	"github.com/onfleet/gonfleet/testingutil"
)

func TestClient_Get(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Setup mock response
	expectedTask := testingutil.GetSampleTask()
	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	// Create task client
	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	// Test Get method
	task, err := client.Get("task_123")

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)
	assert.Equal(t, expectedTask.ShortId, task.ShortId)
	assert.Equal(t, expectedTask.State, task.State)

	// Verify request was made correctly
	mockClient.AssertRequestMade("GET", "/tasks/task_123")
	mockClient.AssertBasicAuth("test_api_key")
}

func TestClient_Get_NotFound(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Setup mock error response
	mockClient.AddResponse("/tasks/nonexistent", testingutil.MockResponse{
		StatusCode: 404,
		Body:       testingutil.GetSampleErrorResponse(),
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	task, err := client.Get("nonexistent")

	assert.Error(t, err)
	assert.Equal(t, "", task.ID) // Empty task on error
}

func TestClient_GetByShortId(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTask := testingutil.GetSampleTask()
	mockClient.AddResponse("/tasks/shortId/abc123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	task, err := client.GetByShortId("abc123")

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)
	assert.Equal(t, expectedTask.ShortId, task.ShortId)

	mockClient.AssertRequestMade("GET", "/tasks/shortId/abc123")
}

func TestClient_List(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTasks := onfleet.TasksPaginated{
		Tasks: []onfleet.Task{
			testingutil.GetSampleTask(),
		},
		LastId: "last_task_123",
	}

	mockClient.AddResponse("/tasks", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTasks,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskListQueryParams{
		From:   1640995200, // 2022-01-01
		To:     1672531199, // 2022-12-31
		Worker: "worker_123",
	}

	tasks, err := client.List(params)

	assert.NoError(t, err)
	assert.Len(t, tasks.Tasks, 1)
	assert.Equal(t, expectedTasks.LastId, tasks.LastId)

	mockClient.AssertRequestMade("GET", "/tasks")
}

func TestClient_ListWithMetadataQuery(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTasks := []onfleet.Task{
		testingutil.GetSampleTask(),
	}

	mockClient.AddResponse("/tasks/metadata", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTasks,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "customer_id",
			Type:  "string",
			Value: "CUST_123",
		},
	}

	tasks, err := client.ListWithMetadataQuery(metadata)

	assert.NoError(t, err)
	assert.Len(t, tasks, 1)
	assert.Equal(t, expectedTasks[0].ID, tasks[0].ID)

	mockClient.AssertRequestMade("POST", "/tasks/metadata")
}

func TestClient_Create(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTask := testingutil.GetSampleTask()
	mockClient.AddResponse("/tasks", testingutil.MockResponse{
		StatusCode: 201,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := testingutil.GetSampleTaskParams()

	task, err := client.Create(params)

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)

	mockClient.AssertRequestMade("POST", "/tasks")
	
	// Verify the request was made correctly
	lastRequest := mockClient.GetLastRequest()
	assert.NotNil(t, lastRequest)
}

func TestClient_Create_ValidationError(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	mockClient.AddResponse("/tasks", testingutil.MockResponse{
		StatusCode: 400,
		Body:       testingutil.GetSampleValidationErrorResponse(),
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	// Invalid params - missing required fields
	params := onfleet.TaskParams{
		PickupTask: false,
		// Missing destination and recipients
	}

	task, err := client.Create(params)

	assert.Error(t, err)
	assert.Equal(t, "", task.ID)
}

func TestClient_BatchCreate(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedResponse := onfleet.TaskBatchCreateResponse{
		Tasks: []onfleet.Task{
			testingutil.GetSampleTask(),
		},
		Errors: []onfleet.TaskBatchCreateError{},
	}

	mockClient.AddResponse("/tasks/batch", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedResponse,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskBatchCreateParams{
		Tasks: []onfleet.TaskParams{
			testingutil.GetSampleTaskParams(),
		},
	}

	response, err := client.BatchCreate(params)

	assert.NoError(t, err)
	assert.Len(t, response.Tasks, 1)
	assert.Len(t, response.Errors, 0)

	mockClient.AssertRequestMade("POST", "/tasks/batch")
}

func TestClient_BatchCreateAsync(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedResponse := onfleet.TaskBatchCreateResponseAsync{
		JobID:  "job_123",
		Status: "PENDING",
	}

	mockClient.AddResponse("/tasks/batch-async", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedResponse,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskBatchCreateParams{
		Tasks: []onfleet.TaskParams{
			testingutil.GetSampleTaskParams(),
		},
	}

	response, err := client.BatchCreateAsync(params)

	assert.NoError(t, err)
	assert.Equal(t, "job_123", response.JobID)
	assert.Equal(t, "PENDING", response.Status)

	mockClient.AssertRequestMade("POST", "/tasks/batch-async")
}

func TestClient_GetBatchJobStatus(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedResponse := onfleet.TaskBatchStatusResponseAsync{
		Status:        "COMPLETED",
		Submitted:     "2023-01-01T00:00:00Z",
		TasksReceived: 1,
		TasksCreated:  1,
		TasksErrored:  0,
		NewTasks: []onfleet.Task{
			testingutil.GetSampleTask(),
		},
		NewTasksWithWarnings: []onfleet.Task{},
		FailedTasks:          []onfleet.TaskParams{},
		Errors:               []onfleet.TaskBatchCreateErrorAsync{},
	}

	mockClient.AddResponse("/tasks/batch/job_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedResponse,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	response, err := client.GetBatchJobStatus("job_123")

	assert.NoError(t, err)
	assert.Equal(t, "COMPLETED", response.Status)
	assert.Equal(t, 1, response.TasksCreated)
	assert.Len(t, response.NewTasks, 1)

	mockClient.AssertRequestMade("GET", "/tasks/batch/job_123")
}

func TestClient_Update(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTask := testingutil.GetSampleTask()
	expectedTask.Notes = "Updated notes"

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskParams{
		Notes: "Updated notes",
	}

	task, err := client.Update("task_123", params)

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)
	assert.Equal(t, "Updated notes", task.Notes)

	mockClient.AssertRequestMade("PUT", "/tasks/task_123")
}

func TestClient_ForceComplete(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	mockClient.AddResponse("/tasks/task_123/complete", testingutil.MockResponse{
		StatusCode: 200,
		Body:       map[string]interface{}{"success": true},
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskForceCompletionParams{
		CompletionDetails: onfleet.TaskForceCompletionDetailsParam{
			Success: true,
			Notes:   "Completed successfully",
		},
	}

	err := client.ForceComplete("task_123", params)

	assert.NoError(t, err)
	mockClient.AssertRequestMade("POST", "/tasks/task_123/complete")
}

func TestClient_Clone(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTask := testingutil.GetSampleTask()
	expectedTask.ID = "cloned_task_456"
	expectedTask.SourceTaskId = "task_123"

	mockClient.AddResponse("/tasks/task_123/clone", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := &onfleet.TaskCloneParams{
		IncludeBarcodes:     true,
		IncludeDependencies: false,
		IncludeMetadata:     true,
		Overrides: &onfleet.TaskCloneOverridesParam{
			Notes: "Cloned task",
		},
	}

	task, err := client.Clone("task_123", params)

	assert.NoError(t, err)
	assert.Equal(t, "cloned_task_456", task.ID)
	assert.Equal(t, "task_123", task.SourceTaskId)

	mockClient.AssertRequestMade("POST", "/tasks/task_123/clone")
}

func TestClient_Clone_NilParams(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedTask := testingutil.GetSampleTask()
	expectedTask.ID = "cloned_task_456"

	mockClient.AddResponse("/tasks/task_123/clone", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	task, err := client.Clone("task_123", nil)

	assert.NoError(t, err)
	assert.Equal(t, "cloned_task_456", task.ID)
}

func TestClient_Delete(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       map[string]interface{}{"success": true},
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	err := client.Delete("task_123")

	assert.NoError(t, err)
	mockClient.AssertRequestMade("DELETE", "/tasks/task_123")
}

func TestClient_Delete_NotFound(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	mockClient.AddResponse("/tasks/nonexistent", testingutil.MockResponse{
		StatusCode: 404,
		Body:       testingutil.GetSampleErrorResponse(),
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	err := client.Delete("nonexistent")

	assert.Error(t, err)
}

func TestClient_AutoAssignMulti(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedResponse := onfleet.TaskAutoAssignMultiResponse{
		AssignedTasksCount: 2,
		AssignedTasks:      []string{"task_123", "task_456"},
	}

	mockClient.AddResponse("/tasks/autoAssign", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedResponse,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskAutoAssignMultiParams{
		Tasks: []string{"task_123", "task_456", "task_789"},
		Options: onfleet.TaskAutoAssignMultiOptionsParam{
			Mode:                         onfleet.TaskAutoAssignModeDistance,
			ConsiderDependencies:         true,
			MaxAssignedTaskCount:         5,
			RestrictAutoAssignmentToTeam: true,
			Teams:                        []string{"team_123"},
		},
	}

	response, err := client.AutoAssignMulti(params)

	assert.NoError(t, err)
	assert.Equal(t, 2, response.AssignedTasksCount)
	assert.Len(t, response.AssignedTasks, 2)
	assert.Equal(t, "task_123", response.AssignedTasks[0])

	mockClient.AssertRequestMade("POST", "/tasks/autoAssign")
}

func TestClient_AutoAssignMulti_NoAssignments(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	expectedResponse := onfleet.TaskAutoAssignMultiResponse{
		AssignedTasksCount: 0,
		AssignedTasks:      []string{},
	}

	mockClient.AddResponse("/tasks/autoAssign", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedResponse,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	params := onfleet.TaskAutoAssignMultiParams{
		Tasks: []string{"task_123"},
		Options: onfleet.TaskAutoAssignMultiOptionsParam{
			Mode: onfleet.TaskAutoAssignModeLoad,
		},
	}

	response, err := client.AutoAssignMulti(params)

	assert.NoError(t, err)
	assert.Equal(t, 0, response.AssignedTasksCount)
	assert.Len(t, response.AssignedTasks, 0)
}

// Table-driven test for different task states
func TestClient_Get_DifferentStates(t *testing.T) {
	tests := []struct {
		name  string
		state onfleet.TaskState
	}{
		{"unassigned task", onfleet.TaskStateUnassigned},
		{"assigned task", onfleet.TaskStateAssigned},
		{"active task", onfleet.TaskStateActive},
		{"completed task", onfleet.TaskStateCompleted},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedTask := testingutil.GetSampleTask()
			expectedTask.State = tt.state

			mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
				StatusCode: 200,
				Body:       expectedTask,
			})

			client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

			task, err := client.Get("task_123")

			assert.NoError(t, err)
			assert.Equal(t, tt.state, task.State)
		})
	}
}

// Test client with different API configurations
func TestClient_DifferentConfigurations(t *testing.T) {
	tests := []struct {
		name   string
		apiKey string
		url    string
	}{
		{"production config", "prod_api_key", "https://onfleet.com/api/v2/tasks"},
		{"staging config", "staging_api_key", "https://staging.onfleet.com/api/v2/tasks"},
		{"custom config", "custom_api_key", "https://custom.onfleet.com/api/v1/tasks"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedTask := testingutil.GetSampleTask()
			mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
				StatusCode: 200,
				Body:       expectedTask,
			})

			client := Plug(tt.apiKey, nil, tt.url, mockClient.MockCaller)

			task, err := client.Get("task_123")

			assert.NoError(t, err)
			assert.Equal(t, expectedTask.ID, task.ID)

			// Verify correct API key was used
			mockClient.AssertBasicAuth(tt.apiKey)
		})
	}
}

func TestClient_List_FilterByState(t *testing.T) {
	tests := []struct {
		name  string
		state string
	}{
		{"unassigned tasks", "0"},
		{"assigned tasks", "1"},
		{"active tasks", "2"},
		{"completed tasks", "3"},
		{"multiple states", "0,1,2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedTasks := onfleet.TasksPaginated{
				Tasks: []onfleet.Task{
					testingutil.GetSampleTask(),
				},
				LastId: "last_task_123",
			}

			mockClient.AddResponse("/tasks", testingutil.MockResponse{
				StatusCode: 200,
				Body:       expectedTasks,
			})

			client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

			params := onfleet.TaskListQueryParams{
				From:  1640995200,
				To:    1672531199,
				State: tt.state,
			}

			tasks, err := client.List(params)

			assert.NoError(t, err)
			assert.Len(t, tasks.Tasks, 1)
			assert.Equal(t, expectedTasks.LastId, tasks.LastId)

			mockClient.AssertRequestMade("GET", "/tasks")
		})
	}
}

func TestClient_List_FilterByContainer(t *testing.T) {
	tests := []struct {
		name       string
		containers string
	}{
		{"single worker container", "worker_123"},
		{"single team container", "team_456"},
		{"multiple containers", "worker_123,team_456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := testingutil.SetupTest(t)
			defer testingutil.CleanupTest(t, mockClient)

			expectedTasks := onfleet.TasksPaginated{
				Tasks: []onfleet.Task{
					testingutil.GetSampleTask(),
				},
				LastId: "last_task_789",
			}

			mockClient.AddResponse("/tasks", testingutil.MockResponse{
				StatusCode: 200,
				Body:       expectedTasks,
			})

			client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

			params := onfleet.TaskListQueryParams{
				From:       1640995200,
				To:         1672531199,
				Containers: tt.containers,
			}

			tasks, err := client.List(params)

			assert.NoError(t, err)
			assert.Len(t, tasks.Tasks, 1)
			assert.Equal(t, expectedTasks.LastId, tasks.LastId)

			mockClient.AssertRequestMade("GET", "/tasks")
		})
	}
}

func TestClient_MetadataSet(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Response after setting the field
	expectedTask := testingutil.GetSampleTask()
	expectedTask.Metadata = []onfleet.Metadata{
		{
			Name:  "pnd_order_num",
			Type:  "string",
			Value: "12345",
		},
	}

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	metadata := []onfleet.Metadata{
		{
			Name:  "pnd_order_num",
			Type:  "string",
			Value: "12345",
		},
	}

	task, err := client.MetadataSet("task_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)

	// Verify the field was set
	assert.Len(t, task.Metadata, 1)
	assert.Equal(t, "pnd_order_num", task.Metadata[0].Name)
	assert.Equal(t, "12345", task.Metadata[0].Value)

	mockClient.AssertRequestMade("PUT", "/tasks/task_123")
}

func TestClient_MetadataSet_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Response after setting: existing field preserved, new field added
	expectedTask := testingutil.GetSampleTask()
	expectedTask.Metadata = []onfleet.Metadata{
		{
			Name:  "existing_field",
			Type:  "string",
			Value: "existing_value",
		},
		{
			Name:  "new_field",
			Type:  "string",
			Value: "new_value",
		},
	}

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	// Set only new_field
	metadata := []onfleet.Metadata{
		{
			Name:  "new_field",
			Type:  "string",
			Value: "new_value",
		},
	}

	task, err := client.MetadataSet("task_123", metadata...)

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)

	// Verify both fields are present (atomicity - existing_field was preserved)
	assert.Len(t, task.Metadata, 2)

	var foundExisting, foundNew bool
	for _, m := range task.Metadata {
		if m.Name == "existing_field" {
			foundExisting = true
			assert.Equal(t, "existing_value", m.Value)
		}
		if m.Name == "new_field" {
			foundNew = true
			assert.Equal(t, "new_value", m.Value)
		}
	}
	assert.True(t, foundExisting, "existing_field should be preserved")
	assert.True(t, foundNew, "new_field should be set")

	mockClient.AssertRequestMade("PUT", "/tasks/task_123")
}

func TestClient_MetadataPop(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Response after popping: field is removed
	expectedTask := testingutil.GetSampleTask()
	expectedTask.Metadata = []onfleet.Metadata{}

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	task, err := client.MetadataPop("task_123", "error")

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)

	// Verify the field was removed
	assert.Len(t, task.Metadata, 0)
	for _, m := range task.Metadata {
		assert.NotEqual(t, "error", m.Name, "error field should be removed")
	}

	mockClient.AssertRequestMade("PUT", "/tasks/task_123")
}

func TestClient_MetadataPop_Atomicity(t *testing.T) {
	mockClient := testingutil.SetupTest(t)
	defer testingutil.CleanupTest(t, mockClient)

	// Response after popping: field_to_remove is gone, field_to_keep remains
	expectedTask := testingutil.GetSampleTask()
	expectedTask.Metadata = []onfleet.Metadata{
		{
			Name:  "field_to_keep",
			Type:  "string",
			Value: "preserved_value",
		},
	}

	mockClient.AddResponse("/tasks/task_123", testingutil.MockResponse{
		StatusCode: 200,
		Body:       expectedTask,
	})

	client := Plug("test_api_key", nil, "https://api.example.com/tasks", mockClient.MockCaller)

	task, err := client.MetadataPop("task_123", "field_to_remove")

	assert.NoError(t, err)
	assert.Equal(t, expectedTask.ID, task.ID)

	// Verify field_to_keep was preserved (atomicity)
	assert.Len(t, task.Metadata, 1)
	assert.Equal(t, "field_to_keep", task.Metadata[0].Name)
	assert.Equal(t, "preserved_value", task.Metadata[0].Value)

	// Verify field_to_remove is not present
	for _, m := range task.Metadata {
		assert.NotEqual(t, "field_to_remove", m.Name, "field_to_remove should not be present")
	}

	mockClient.AssertRequestMade("PUT", "/tasks/task_123")
}