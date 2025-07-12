package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/redhat-data-and-ai/usernaut/pkg/common/structs"
)

// FetchAllUsers fetches all users from Snowflake using REST API with proper pagination
// Snowflake pagination works as follows:
// 1. First call /api/v2/users - returns first page + Link header with result ID
// 2. Subsequent calls /api/v2/results/{result_id}?page=N - returns additional pages
func (c *SnowflakeClient) FetchAllUsers(ctx context.Context) (map[string]*structs.User, map[string]*structs.User, error) {
	resultByID := make(map[string]*structs.User)
	resultByEmail := make(map[string]*structs.User)

	// First request to get initial page and Link header
	resp, headers, status, err := c.sendRequest(ctx, "/api/v2/users", http.MethodGet, nil)
	if err != nil {
		return nil, nil, err
	}
	if status != http.StatusOK {
		return nil, nil, fmt.Errorf("failed to fetch users, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Process first page
	if err := c.processUsersPage(resp, resultByID, resultByEmail); err != nil {
		return nil, nil, err
	}

	// Check for additional pages in Link header
	linkHeader := headers.Get("Link")
	if linkHeader != "" {
		nextURL := parseLinkHeader(linkHeader, "next")

		// Follow pagination using Link header URLs
		for nextURL != "" {
			resp, headers, status, err := c.sendRequest(ctx, nextURL, http.MethodGet, nil)
			if err != nil {
				return nil, nil, err
			}
			if status != http.StatusOK {
				break // End of pagination
			}

			// Process this page
			if err := c.processUsersPage(resp, resultByID, resultByEmail); err != nil {
				return nil, nil, err
			}

			// Get next page URL
			linkHeader = headers.Get("Link")
			if linkHeader != "" {
				nextURL = parseLinkHeader(linkHeader, "next")
			} else {
				nextURL = ""
			}
		}
	}

	return resultByID, resultByEmail, nil
}

// processUsersPage processes a page of users and adds them to the result maps
func (c *SnowflakeClient) processUsersPage(resp []byte, resultByID map[string]*structs.User, resultByEmail map[string]*structs.User) error {
	// Parse the response - expecting an array of users
	var users []map[string]interface{}
	if err := json.Unmarshal(resp, &users); err != nil {
		return fmt.Errorf("failed to parse users response: %w", err)
	}

	// Extract users from the response
	for _, userMap := range users {
		if name, ok := userMap["name"].(string); ok {
			user := &structs.User{
				ID:       name,
				UserName: name,
			}

			// Extract email if available
			if email, ok := userMap["email"].(string); ok {
				user.Email = email
			}

			// Extract display name if available
			if displayName, ok := userMap["displayName"].(string); ok {
				user.DisplayName = displayName
			}

			resultByID[name] = user
			if user.Email != "" {
				resultByEmail[user.Email] = user
			}
		}
	}

	return nil
}

// CreateUser creates a new user in Snowflake using REST API
func (c *SnowflakeClient) CreateUser(ctx context.Context, user *structs.User) (*structs.User, error) {
	endpoint := "/api/v2/users"

	// Create payload for user creation
	payload := map[string]interface{}{
		"name": user.UserName,
	}

	// Add optional fields if provided
	if user.Email != "" {
		payload["email"] = user.Email
	}
	if user.DisplayName != "" {
		payload["displayName"] = user.DisplayName
	}

	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodPost, payload)
	if err != nil {
		return nil, err
	}

	// Check for successful creation
	if status != http.StatusOK && status != http.StatusCreated {
		return nil, fmt.Errorf("failed to create user, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Parse response to get created user details
	var response map[string]interface{}
	if err := json.Unmarshal(resp, &response); err != nil {
		return nil, fmt.Errorf("failed to parse create user response: %w", err)
	}

	// Return the created user
	createdUser := &structs.User{
		ID:          user.UserName,
		UserName:    user.UserName,
		Email:       user.Email,
		DisplayName: user.DisplayName,
	}

	return createdUser, nil
}

// FetchUserDetails fetches details for a specific user using REST API
func (c *SnowflakeClient) FetchUserDetails(ctx context.Context, userID string) (*structs.User, error) {
	endpoint := fmt.Sprintf("/api/v2/users/%s", userID)
	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch user details, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Parse the response
	var response map[string]interface{}
	if err := json.Unmarshal(resp, &response); err != nil {
		return nil, fmt.Errorf("failed to parse user response: %w", err)
	}

	user := &structs.User{
		ID:       userID,
		UserName: userID,
	}

	// Extract user details from response
	if email, ok := response["email"].(string); ok {
		user.Email = email
	}
	if displayName, ok := response["displayName"].(string); ok {
		user.DisplayName = displayName
	}

	return user, nil
}

// UpdateUser updates an existing user in Snowflake using REST API
func (c *SnowflakeClient) UpdateUser(ctx context.Context, user *structs.User) (*structs.User, error) {
	endpoint := fmt.Sprintf("/api/v2/users/%s", user.UserName)

	// Create payload for user update
	payload := map[string]interface{}{}

	// Add fields to update
	if user.Email != "" {
		payload["email"] = user.Email
	}
	if user.DisplayName != "" {
		payload["displayName"] = user.DisplayName
	}

	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodPut, payload)
	if err != nil {
		return nil, err
	}

	// Check for successful update
	if status != http.StatusOK {
		return nil, fmt.Errorf("failed to update user, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Return the updated user
	return user, nil
}

// DeleteUser deletes a user from Snowflake using REST API
func (c *SnowflakeClient) DeleteUser(ctx context.Context, userID string) error {
	endpoint := fmt.Sprintf("/api/v2/users/%s", userID)

	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodDelete, nil)
	if err != nil {
		return fmt.Errorf("failed to delete user: %w", err)
	}

	// Check for successful deletion
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("failed to delete user, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	return nil
}
