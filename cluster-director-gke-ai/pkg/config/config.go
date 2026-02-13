// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"context"
	"fmt"

	"cluster-director-mcp/genericCore"

	"golang.org/x/oauth2/google"
)

type Config struct {
	userAgent        string
	defaultProjectID string
	defaultZone      string
	defaultRegion    string
}

func (c *Config) UserAgent() string {
	return c.userAgent
}

func (c *Config) GetDefaultProjectID() string {
	return c.defaultProjectID
}

func (c *Config) SetDefaultProjectID(p string) {
	c.defaultProjectID = p
}

func (c *Config) GetDefaultZone() string {
	return c.defaultZone
}

func (c *Config) SetDefaultZone(p string) {
	c.defaultZone = p
}

func (c *Config) GetDefaultRegion() string {
	return c.defaultRegion
}

func (c *Config) SetDefaultRegion(p string) {
	c.defaultRegion = p
}

func New(version string) *Config {
	return &Config{
		userAgent:        "cluster-director-gke-ai/" + version,
		defaultProjectID: getDefaultProjectID(),
	}
}

// [UPDATED] Replaced exec.Command("gcloud"...) with native SDK credential detection.
// This aligns with the "google_credentials" authProviderType best practices.
func getDefaultProjectID() string {
	ctx := context.Background()

	// FindDefaultCredentials natively checks environment variables, 
	// the Metadata Server, or local ADC files without spawning a shell.
	credentials, err := google.FindDefaultCredentials(ctx)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Failed to find default credentials: %v", err))
		return ""
	}

	// The SDK automatically extracts the ProjectID from the active environment.
	projectID := credentials.ProjectID
	if projectID == "" {
		genericCore.WriteToLog("Default project ID not found in environment credentials.")
		return ""
	}

	genericCore.WriteToLog(fmt.Sprintf("Using natively detected default project ID: %s", projectID))
	return projectID
}