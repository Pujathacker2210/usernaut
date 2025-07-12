/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package snowflake

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/redhat-data-and-ai/usernaut/pkg/common/structs"
)

// FetchUserTeams fetches all teams a user belongs to
func (c *SnowflakeClient) FetchUserTeams(ctx context.Context, userID string) (map[string]structs.Team, error) {
	endpoint := fmt.Sprintf("/api/v2/users/%s/grants", userID)
	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch user teams, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Parse the response - expecting an array of grants
	var grants []map[string]interface{}
	if err := json.Unmarshal(resp, &grants); err != nil {
		return nil, fmt.Errorf("failed to parse user teams response: %w", err)
	}

	// Extract teams from the grants
	teams := make(map[string]structs.Team)
	for _, grant := range grants {
		if privilege, ok := grant["privilege"].(string); ok && privilege == "USAGE" {
			if securable, ok := grant["securable"].(map[string]interface{}); ok {
				if name, ok := securable["name"].(string); ok {
					team := structs.Team{
						ID:   name,
						Name: name,
					}
					teams[name] = team
				}
			}
		}
	}

	return teams, nil
}

// FetchTeamMembersByTeamID fetches all members of a specific team
func (c *SnowflakeClient) FetchTeamMembersByTeamID(ctx context.Context, teamID string) (map[string]*structs.User, error) {
	endpoint := fmt.Sprintf("/api/v2/roles/%s/grants-on", teamID)
	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("failed to fetch team members, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Parse the response - expecting an array of grants
	var grants []map[string]interface{}
	if err := json.Unmarshal(resp, &grants); err != nil {
		return nil, fmt.Errorf("failed to parse team members response: %w", err)
	}

	// Extract users from the grants
	users := make(map[string]*structs.User)
	for _, grant := range grants {
		if grantee, ok := grant["grantee"].(map[string]interface{}); ok {
			if name, ok := grantee["name"].(string); ok && grantee["type"].(string) == "USER" {
				user := &structs.User{
					ID:       name,
					UserName: name,
				}
				users[name] = user
			}
		}
	}

	return users, nil
}

// AddUserToTeam adds a user to a team (grants role to user)
func (c *SnowflakeClient) AddUserToTeam(ctx context.Context, userID, teamID string) error {
	endpoint := fmt.Sprintf("/api/v2/users/%s/grants", userID)

	// Create payload for granting role to user
	payload := map[string]interface{}{
		"securable": map[string]string{
			"name": teamID,
		},
		"containing_scope": map[string]string{
			"database": "DEFAULT",
		},
		"securable_type": "ROLE",
		"privileges":     []string{},
	}

	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodPost, payload)
	if err != nil {
		return err
	}

	// Check for successful grant
	if status != http.StatusOK && status != http.StatusCreated {
		return fmt.Errorf("failed to add user to team, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	return nil
}

// RemoveUserFromTeam removes a user from a team (revokes role from user)
func (c *SnowflakeClient) RemoveUserFromTeam(ctx context.Context, userID, teamID string) error {
	endpoint := fmt.Sprintf("/api/v2/users/%s/grants:revoke", userID)

	// Create payload for revoking role from user
	payload := map[string]interface{}{
		"securable": map[string]string{
			"name": teamID,
		},
		"containing_scope": map[string]string{
			"database": "DEFAULT",
		},
		"securable_type": "ROLE",
		"privileges":     []string{},
	}

	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodPost, payload)
	if err != nil {
		return err
	}

	// Check for successful revocation
	if status != http.StatusOK && status != http.StatusNoContent {
		return fmt.Errorf("failed to remove user from team, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	return nil
}

// IsUserInTeam checks if a user is a member of a specific team
func (c *SnowflakeClient) IsUserInTeam(ctx context.Context, userID, teamID string) (bool, error) {
	endpoint := fmt.Sprintf("/api/v2/users/%s/grants", userID)
	resp, _, status, err := c.sendRequest(ctx, endpoint, http.MethodGet, nil)
	if err != nil {
		return false, err
	}
	if status != http.StatusOK {
		return false, fmt.Errorf("failed to check user team membership, status: %s, body: %s", http.StatusText(status), string(resp))
	}

	// Parse the response - expecting an array of grants
	var grants []map[string]interface{}
	if err := json.Unmarshal(resp, &grants); err != nil {
		return false, fmt.Errorf("failed to parse user grants response: %w", err)
	}

	// Check if user has the specific role
	for _, grant := range grants {
		if securable, ok := grant["securable"].(map[string]interface{}); ok {
			if name, ok := securable["name"].(string); ok && name == teamID {
				return true, nil
			}
		}
	}

	return false, nil
}
