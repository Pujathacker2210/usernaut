package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redhat-data-and-ai/usernaut/pkg/clients/snowflake"
	"github.com/redhat-data-and-ai/usernaut/pkg/common/structs"
	"github.com/redhat-data-and-ai/usernaut/pkg/config"
)

// Helper function to get the first key from a map
func getFirstKey(users map[string]*structs.User) string {
	for key := range users {
		return key
	}
	return ""
}

func main() {
	fmt.Println("🚀 Starting Snowflake Backend Local Tests...")

	// Set environment to use snowflake.yaml config
	os.Setenv("APP_ENV", "snowflake")

	// Load app config (this loads your YAML and env vars)
	appConfig, err := config.GetConfig()
	if err != nil {
		fmt.Printf("❌ Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Find your Snowflake backend config
	backend, ok := appConfig.BackendMap["snowflake"]["snowflake_rhplatformtest"]
	if !ok {
		fmt.Println("❌ Snowflake backend config not found")
		os.Exit(1)
	}

	fmt.Printf("✅ Config loaded successfully. Base URL: %s\n", backend.GetStringConnection("base_url", ""))

	// Build the Snowflake config struct
	snowflakeCfg := snowflake.SnowflakeConfig{
		Account: backend.GetStringConnection("account", ""),
		PAT:     backend.GetStringConnection("pat", ""),
		BaseURL: backend.GetStringConnection("base_url", ""),
	}

	// Create the client
	client, err := snowflake.NewClient(
		snowflakeCfg,
		appConfig.HttpClient.ConnectionPoolConfig,
		appConfig.HttpClient.HystrixResiliencyConfig,
	)
	if err != nil {
		fmt.Printf("❌ Failed to create Snowflake client: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ Snowflake client created successfully!")

	// Test all functions
	ctx := context.Background()
	var createdTeam *structs.Team

	// 1. Test FetchAllUsers
	fmt.Println("📋 Testing FetchAllUsers...")
	users, usersByEmail, err := client.FetchAllUsers(ctx)
	if err != nil {
		fmt.Printf("❌ FetchAllUsers failed: %v\n", err)
		return
	}
	fmt.Printf("✅ Found %d users (by ID), %d users (by email)\n", len(users), len(usersByEmail))
	if len(users) > 0 {
		fmt.Printf("   Sample user: %s\n", getFirstKey(users))
	}

	// 2. Test FetchAllTeams
	fmt.Println("\n🏢 Testing FetchAllTeams...")
	teams, err := client.FetchAllTeams(ctx)
	if err != nil {
		fmt.Printf("❌ FetchAllTeams failed: %v\n", err)
	} else {
		fmt.Printf("✅ FetchAllTeams succeeded! Found %d teams\n", len(teams))

		// Show first few teams
		count := 0
		for id, team := range teams {
			if count < 3 {
				fmt.Printf("   - Team: ID=%s, Name=%s\n", id, team.Name)
				count++
			}
		}
	}

	// 3. Test CreateTeam (with unique name)
	fmt.Println("\n➕ Testing CreateTeam...")
	// Create test team with a unique name
	testTeamName := fmt.Sprintf("USERNAUT_TEST_TEAM_%d", time.Now().Unix())
	testTeam := &structs.Team{
		Name: testTeamName,
	}

	createdTeam, err = client.CreateTeam(ctx, testTeam)
	if err != nil {
		fmt.Printf("❌ CreateTeam failed: %v\n", err)
	} else {
		fmt.Printf("✅ CreateTeam succeeded! Created team: ID=%s, Name=%s\n", createdTeam.ID, createdTeam.Name)
	}

	// 6. Test CreateUser (test independently)
	fmt.Println("\n👤 Testing CreateUser...")
	testUser := &structs.User{
		UserName:    "USERNAUT_TEST_USER",
		Email:       "test@usernaut.dev",
		DisplayName: "Test User",
	}

	createdUser, err := client.CreateUser(ctx, testUser)
	if err != nil {
		fmt.Printf("❌ CreateUser failed: %v\n", err)
	} else {
		fmt.Printf("✅ CreateUser succeeded! Created user: ID=%s, Name=%s\n", createdUser.ID, createdUser.UserName)

		// 7. Test FetchUserDetails
		fmt.Println("\n🔍 Testing FetchUserDetails...")
		userDetails, err := client.FetchUserDetails(ctx, createdUser.ID)
		if err != nil {
			fmt.Printf("❌ FetchUserDetails failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchUserDetails succeeded! User: ID=%s, Email=%s\n", userDetails.ID, userDetails.Email)
		}

		// Test with existing team if CreateTeam succeeded
		if createdTeam != nil {
			// 8. Test AddUserToTeam
			fmt.Println("\n➕ Testing AddUserToTeam...")
			err = client.AddUserToTeam(ctx, createdUser.ID, createdTeam.ID)
			if err != nil {
				fmt.Printf("❌ AddUserToTeam failed: %v\n", err)
			} else {
				fmt.Printf("✅ AddUserToTeam succeeded! Added user %s to team %s\n", createdUser.ID, createdTeam.ID)
			}

			// 9. Test RemoveUserFromTeam
			fmt.Println("\n➖ Testing RemoveUserFromTeam...")
			err = client.RemoveUserFromTeam(ctx, createdUser.ID, createdTeam.ID)
			if err != nil {
				fmt.Printf("❌ RemoveUserFromTeam failed: %v\n", err)
			} else {
				fmt.Printf("✅ RemoveUserFromTeam succeeded! Removed user %s from team %s\n", createdUser.ID, createdTeam.ID)
			}
		}

		// Cleanup: Delete test user
		fmt.Println("\n🧹 Cleaning up test user...")
		err = client.DeleteUser(ctx, createdUser.ID)
		if err != nil {
			fmt.Printf("⚠️  Failed to cleanup test user: %v\n", err)
		} else {
			fmt.Printf("✅ Test user deleted successfully\n")
		}
	}

	// Test with existing team if CreateTeam succeeded
	if createdTeam != nil {
		// 10. Test FetchTeamDetails
		fmt.Println("\n🔍 Testing FetchTeamDetails...")
		teamDetails, err := client.FetchTeamDetails(ctx, createdTeam.ID)
		if err != nil {
			fmt.Printf("❌ FetchTeamDetails failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchTeamDetails succeeded! Team: ID=%s, Name=%s\n", teamDetails.ID, teamDetails.Name)
		}

		// 11. Test FetchTeamMembersByTeamID
		fmt.Println("\n👥 Testing FetchTeamMembersByTeamID...")
		members, err := client.FetchTeamMembersByTeamID(ctx, createdTeam.ID)
		if err != nil {
			fmt.Printf("❌ FetchTeamMembersByTeamID failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchTeamMembersByTeamID succeeded! Found %d members\n", len(members))
		}

		// Cleanup: Delete test team - COMMENTED OUT FOR INSPECTION
		// fmt.Println("\n🧹 Cleaning up test team...")
		// err = client.DeleteTeamByID(ctx, createdTeam.ID)
		// if err != nil {
		// 	fmt.Printf("⚠️  Failed to cleanup test team: %v\n", err)
		// } else {
		// 	fmt.Printf("✅ Test team deleted successfully\n")
		// }
		fmt.Println("\n🔍 Test team left in Snowflake for inspection:", createdTeam.ID)
	}

	// Test read-only functions with existing data
	fmt.Println("\n🔍 Testing read-only functions with existing data...")

	// Test FetchUserDetails with existing user
	if len(users) > 0 {
		existingUserID := getFirstKey(users)
		fmt.Printf("\n👤 Testing FetchUserDetails with existing user: %s\n", existingUserID)
		userDetails, err := client.FetchUserDetails(ctx, existingUserID)
		if err != nil {
			fmt.Printf("❌ FetchUserDetails failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchUserDetails succeeded! User: ID=%s, Email=%s, DisplayName=%s\n",
				userDetails.ID, userDetails.Email, userDetails.DisplayName)
		}
	}

	// Test FetchTeamDetails with existing team
	if len(teams) > 0 {
		existingTeamID := ""
		for id := range teams {
			existingTeamID = id
			break
		}
		fmt.Printf("\n🏢 Testing FetchTeamDetails with existing team: %s\n", existingTeamID)
		teamDetails, err := client.FetchTeamDetails(ctx, existingTeamID)
		if err != nil {
			fmt.Printf("❌ FetchTeamDetails failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchTeamDetails succeeded! Team: ID=%s, Name=%s\n",
				teamDetails.ID, teamDetails.Name)
		}

		// Test FetchTeamMembersByTeamID with existing team
		fmt.Printf("\n👥 Testing FetchTeamMembersByTeamID with existing team: %s\n", existingTeamID)
		members, err := client.FetchTeamMembersByTeamID(ctx, existingTeamID)
		if err != nil {
			fmt.Printf("❌ FetchTeamMembersByTeamID failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchTeamMembersByTeamID succeeded! Found %d members\n", len(members))
			if len(members) > 0 {
				for memberID := range members {
					fmt.Printf("   - Member: %s\n", memberID)
					break // Just show first member
				}
			}
		}
	}

	// Test FetchUserTeams with existing user
	if len(users) > 0 {
		existingUserID := getFirstKey(users)
		fmt.Printf("\n👤 Testing FetchUserTeams with existing user: %s\n", existingUserID)
		userTeams, err := client.FetchUserTeams(ctx, existingUserID)
		if err != nil {
			fmt.Printf("❌ FetchUserTeams failed: %v\n", err)
		} else {
			fmt.Printf("✅ FetchUserTeams succeeded! User has %d team memberships\n", len(userTeams))
			count := 0
			for teamID := range userTeams {
				if count < 3 {
					fmt.Printf("   - Team: %s\n", teamID)
					count++
				}
			}
		}
	}

	fmt.Println("\n🎉 All tests completed!")
}
