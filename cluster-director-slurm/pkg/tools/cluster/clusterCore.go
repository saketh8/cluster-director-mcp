// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/user"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"cluster-director-mcp/genericCore"
	"cluster-director-mcp/persistence"

	"golang.org/x/oauth2/google"

	compute "google.golang.org/api/compute/v0.alpha"
)

var authToken string

// One-2-Many Mapping, i.e one region maps to a one or more zones
var regions2Zones = make(map[string][]string)

// The root struct that holds information on a list of clusters.
type ClustersResponse struct {
	Clusters []Cluster `json:"clusters"`
}

// Key to this map is the clusters region
var MostRecentClusterData = make(map[string]*ClustersResponse)

var region2ClusterNames = make(map[string][]string)
var clusterNames2JSON = make(map[string]string)
var clusterNames2Zone = make(map[string]string)

// Cluster defines the top-level structure of the JSON object returned
// from Cluster Director API
type Cluster struct {
	Name         string       `json:"name"`
	CreateTime   string       `json:"createTime"`
	UpdateTime   string       `json:"updateTime"`
	Networks     []Network    `json:"networks"`
	Storages     []Storage    `json:"storages"`
	Compute      Compute      `json:"compute"`
	Orchestrator Orchestrator `json:"orchestrator"`
	Reconciling  bool         `json:"reconciling"`
}

// Network corresponds to an object in the "networks" array.
type Network struct {
	Network          string `json:"network"`
	InitializeParams struct {
		Network string `json:"network"`
	} `json:"initializeParams"`
	Subnetwork string `json:"subnetwork"`
}

// Storage corresponds to an object in the "storages" array.
type Storage struct {
	Storage          string `json:"storage"`
	InitializeParams struct {
		Filestore struct {
			FileShares []struct {
				CapacityGb string `json:"capacityGb"`
				FileShare  string `json:"fileShare"`
			} `json:"fileShares"`
			Tier      string `json:"tier"`
			Filestore string `json:"filestore"`
			Protocol  string `json:"protocol"`
		} `json:"filestore"`
	} `json:"initializeParams"`
	ID string `json:"id"`
}

// Compute corresponds to the "compute" object.
type Compute struct {
	ResourceRequests []ResourceRequest `json:"resourceRequests"`
}

// ResourceRequest corresponds to an object in the "resourceRequests" array.
type ResourceRequest struct {
	ID                string                   `json:"id"`
	Zone              string                   `json:"zone"`
	MachineType       string                   `json:"machineType"`
	GuestAccelerators []map[string]interface{} `json:"guestAccelerators"`
	Disks             []Disk                   `json:"disks"`
	ProvisioningModel string                   `json:"provisioningModel"`
}

// Disk corresponds to a disk object.
type Disk struct {
	Type        string `json:"type"`
	SizeGb      string `json:"sizeGb"`
	Boot        bool   `json:"boot"`
	SourceImage string `json:"sourceImage"`
}

// Orchestrator corresponds to the "orchestrator" object.
type Orchestrator struct {
	Slurm Slurm `json:"slurm"`
}

// Slurm corresponds to the "slurm" object.
type Slurm struct {
	NodeSets         []NodeSet   `json:"nodeSets"`
	Partitions       []Partition `json:"partitions"`
	DefaultPartition string      `json:"defaultPartition"`
	LoginNodes       LoginNodes  `json:"loginNodes"`
}

// NodeSet corresponds to an object in the "nodeSets" array.
type NodeSet struct {
	ID                string          `json:"id"`
	ResourceRequestID string          `json:"resourceRequestId"`
	StorageConfigs    []StorageConfig `json:"storageConfigs"`
	StaticNodeCount   string          `json:"staticNodeCount"`
	EnableOsLogin     bool            `json:"enableOsLogin"`
}

// Partition corresponds to an object in the "partitions" array.
type Partition struct {
	ID         string   `json:"id"`
	NodeSetIDs []string `json:"nodeSetIds"`
}

// LoginNodes corresponds to the "loginNodes" object.
type LoginNodes struct {
	MachineType     string `json:"machineType"`
	Zone            string `json:"zone"`
	Count           string `json:"count"`
	Disks           []Disk `json:"disks"`
	EnableOsLogin   bool   `json:"enableOsLogin"`
	EnablePublicIps bool   `json:"enablePublicIps"`
	Instances       []struct {
		Instance string `json:"instance"`
	} `json:"instances"`
	StorageConfigs []StorageConfig `json:"storageConfigs"`
}

// StorageConfig corresponds to a storage configuration object.
type StorageConfig struct {
	ID         string `json:"id"`
	LocalMount string `json:"localMount"`
}

func getGCloudToken() bool {
	if authToken != "" {
		return true
	}

	genericCore.WriteToLog("Retrieving native Google OAuth2 token...")

	ctx := context.Background()
	// Scopes for Compute and Cloud Platform access
	scopes := []string{
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/compute",
	}

	creds, err := google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error finding default credentials: %v", err))
		return false
	}

	token, err := creds.TokenSource.Token()
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error retrieving token: %v", err))
		return false
	}

	authToken = token.AccessToken
	genericCore.WriteToLog("Successfully retrieved native access token.")
	return true
}

func getAllZonesInRegion(region string, projectID string, ctx context.Context, computeService *compute.Service) []string {
	var zonesList []string

	// Ensure we only get zones for the specific region (e.g., us-central1-a, us-central1-b)
	filter := fmt.Sprintf("name=%s-*", region)

	req1 := computeService.Zones.List(projectID).Filter(filter)

	if err := req1.Pages(ctx, func(page *compute.ZoneList) error {
		for _, zone := range page.Items {
			zonesList = append(zonesList, zone.Name)
		}
		return nil
	}); err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error getting zones for project %s in region %s : %v",
			projectID, region, err))
	}
	return zonesList
}

func getAllRegionsAndZonesSupportedByHCS(projectID string) bool {
	// 1. Define Structs with EXACT JSON tags used by the v1 API
	type Location struct {
		Name       string `json:"name"`
		LocationId string `json:"locationId"` // 'locationId' must have a lowercase 'd'
	}

	type LocationList struct {
		Locations []Location `json:"locations"`
	}

	var locationData LocationList

	// 2. Ensure native auth token is available
	if !getGCloudToken() {
		genericCore.WriteToLog("Native token acquisition failed in getAllRegionsAndZonesSupportedByHCS")
		return false
	}

	// 3. UPDATED URL: Using v1 GA endpoint and the correct 'locations' resource path
	url := fmt.Sprintf("https://hypercomputecluster.googleapis.com/v1/projects/%s/locations", projectID)

	bodyJson, success := genericCore.QueryURLAndGetResult(authToken, url)
	if !success {
		genericCore.WriteToLog("Error calling Cluster Director Locations API at: " + url)
		return false
	}

	// Log raw response for debugging purposes
	genericCore.WriteToLog("RAW HCS Locations JSON: " + bodyJson)

	// 4. Unmarshal into the struct
	err := json.Unmarshal([]byte(bodyJson), &locationData)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error unmarshaling HCS locations JSON: %v", err))
		return false
	}

	// Verify discovery
	if len(locationData.Locations) == 0 {
		genericCore.WriteToLog("HCS API returned 0 locations. Is the service enabled for project " + projectID + "?")
		return false
	}

	// 5. Setup Native Compute Client
	ctx := context.Background()
	computeService, err := compute.NewService(ctx)
	if err != nil {
		genericCore.WriteToLog(fmt.Sprintf("Error initializing Native Compute Service: %v", err))
		return false
	}

	// 6. Map Regions to Zones
	for _, loc := range locationData.Locations {
		// Use LocationId to populate the global mapping
		if loc.LocationId != "" {
			regions2Zones[loc.LocationId] = getAllZonesInRegion(loc.LocationId, projectID, ctx, computeService)
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Successfully synchronized %d regions for Cluster Director.", len(locationData.Locations)))
	return true
}

// Return zone for a cluster if it is present in the cached data structure
func getZoneForCluster(projectID string, clusterName string) string {
	var zone string

	zone, exists := clusterNames2Zone[clusterName]
	if exists {
		return zone
	} else {
		getClustersInAllRegions(projectID)
		zone, exists = clusterNames2Zone[clusterName]
		if exists {
			return zone
		} else {
			genericCore.WriteToLog("Error getting zone for cluster " + clusterName + " in project " + projectID)
			return ""
		}
	}
}

func getClustersInAllRegions(projectID string) (string, int) {
	var listOfClusters string = "["
	countClusters := 0
	if len(regions2Zones) == 0 {
		getAllRegionsAndZonesSupportedByHCS(projectID)
	}
	genericCore.WriteToLog(fmt.Sprintf("Getting clusters in all regions for projectId : %s", projectID))
	for region, _ := range regions2Zones {
		genericCore.WriteToLog(fmt.Sprintf("Getting clusters in region : %s", region))
		getClustersInRegionIfExists(region, projectID)
		for _, clusterName := range region2ClusterNames[region] {
			genericCore.WriteToLog(fmt.Sprintf("Found cluster : %s", clusterName))
			listOfClusters += string("\"" + clusterName + "\", ")
			countClusters++
		}
	}
	listOfClusters = strings.TrimSuffix(listOfClusters, ", ")
	listOfClusters += "]"

	genericCore.WriteToLog(fmt.Sprintf("Count of clusters found: %d", countClusters))
	genericCore.WriteToLog(fmt.Sprintf("Final list of clusters in all regions in project %s : %s ", projectID, listOfClusters))

	return listOfClusters, countClusters
}

func getClustersInRegionIfExists(region string, projectID string) {
	// 1. Ensure the OAuth2 token is valid using the native Go SDK logic
	// This replaces the need to manually run 'gcloud auth print-access-token'
	if !getGCloudToken() {
		genericCore.WriteToLog("getClustersInRegionIfExists - Failed to acquire native auth token")
		return
	}

	url := "https://hypercomputecluster.googleapis.com/v1/projects/" + projectID + "/locations/" + region + "/clusters"
	genericCore.WriteToLog(fmt.Sprintf("getClustersInRegionIfExists - Getting clusters in region %s URL : %s", region, url))

	// Clear previous cache for this region to ensure data consistency
	region2ClusterNames[region] = []string{}

	// 2. Query the Cluster Director API using the native token
	bodyString, success := genericCore.QueryURLAndGetResult(authToken, url)
	genericCore.WriteToLog(fmt.Sprintf("Response received from Cluster Director API for region %s (Payload Size: %d bytes)", region, len(bodyString)))

	// 3. Process the response
	// The check for "storages" ensures the API returned a valid cluster object list
	if success && bodyString != "" {
		genericCore.WriteToLog("Trying to parse JSON response...")
		var parsedClusterData ClustersResponse
		err := json.Unmarshal([]byte(bodyString), &parsedClusterData)

		if err != nil {
			genericCore.WriteToLog(fmt.Sprintf("Error unmarshalling cluster JSON: %v", err))
		} else if len(parsedClusterData.Clusters) == 0 {
			genericCore.WriteToLog(fmt.Sprintf("No clusters found in region %s", region))
		} else {
			// Update the global cache pointer
			MostRecentClusterData[region] = &parsedClusterData

			genericCore.WriteToLog(fmt.Sprintf("Found %d clusters in region %s", len(MostRecentClusterData[region].Clusters), region))

			for i := range MostRecentClusterData[region].Clusters {
				cluster := MostRecentClusterData[region].Clusters[i]
				clusterName := filepath.Base(cluster.Name)

				genericCore.WriteToLog(fmt.Sprintf("Processing data for cluster: %s", clusterName))

				// Map the cluster to its respective zone from the ResourceRequests
				for j := range cluster.Compute.ResourceRequests {
					clusterZone := cluster.Compute.ResourceRequests[j].Zone
					if clusterZone != "" {
						genericCore.WriteToLog(fmt.Sprintf("Mapping cluster %s to zone %s", clusterName, clusterZone))
						clusterNames2Zone[clusterName] = clusterZone
					}
				}

				// Update internal mapping structures
				region2ClusterNames[region] = append(region2ClusterNames[region], clusterName)
				clusterNames2JSON[clusterName] = bodyString
			}
		}
	} else {
		genericCore.WriteToLog(fmt.Sprintf("No active clusters found or invalid response for region %s", region))
	}
}

// Instance holds the parsed data for a single machine.
type Instance struct {
	Name        string
	MachineType string
}

var AllInstancesInProject []Instance

// PartitionInfo holds the structured data for a single line from the slurm command "sinfo"
// sample output of "sinfo"
// PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
// part1*       up   infinite      1  idle# xxxxx-nodeset1-0
// part1*       up   infinite      3   idle xxxxx-nodeset1-[1-3]
type PartitionInfo struct {
	Name      string
	IsDefault bool
	Avail     string
	TimeLimit string
	Nodes     int
	State     string
	NodeList  string
}

func parseOutputofSlurmSinfoCmdAndReturnPartitions(output string) (map[string][]string, map[string]struct{}, bool) {
	genericCore.WriteToLog("Raw output from sinfo : " + output)

	var partitions = make(map[string][]string)
	var clusterStates = make(map[string]struct{})

	// Split the output into lines and trim any surrounding whitespace.
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Make sure we have more than just the header line.
	if len(lines) < 2 {
		return nil, nil, false
	}

	// Iterate over the lines, skipping the header at index 0.
	for _, line := range lines[1:] {
		genericCore.WriteToLog("Parsing sinfo line: " + line)
		if strings.Contains(line, "PARTITION") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 6 {
			// Skip any malformed or empty lines.
			continue
		}

		// trim any chars that is not alphabet or letter
		partitionName := strings.TrimRightFunc(fields[0], func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsNumber(r) })
		genericCore.WriteToLog("Parsed partition name : " + partitionName)

		partitions[partitionName] = append(partitions[partitionName], fields[5])

		// state is something like idle#, idle~ down, alloc ...etc
		clusterStates[fields[4]] = struct{}{}
	}

	return partitions, clusterStates, true
}

func GetDetailedJobInfoForAllRunningCDMcpJobsOfUserInCluster(projectId string,
	clusterName string,
	zone string,
	loginNode string) (map[int]int, bool, string) {

	// Key is cluster-director-mc Job Id, value is slurm Job Id
	var jobDataMap = make(map[int]int)

	jobArray, errorMsg, success := GetRunningSlurmJobsForUserInCluster(projectId, clusterName, zone, loginNode)
	if !success {
		return jobDataMap, false, errorMsg
	}

	r := regexp.MustCompile(`CDMcpJobId\.(\d+)`)
	for _, slurmJobId := range jobArray {
		scontrolCmd := fmt.Sprintf("scontrol show job -dd %d", slurmJobId)
		sshOut, success := runSSHOnNode(loginNode, projectId, zone, scontrolCmd)
		if !success {
			s := string(fmt.Sprintf("Could not run scontrol to get info for job %d", slurmJobId) +
				" on login node " + loginNode + " in cluster " + clusterName + " in project " + projectId + "\n" + genericCore.GetLastLines(sshOut, 10))
			genericCore.WriteToLog(s)
			return jobDataMap, false, s
		}

		if strings.Contains(sshOut, "/"+strings.ReplaceAll(persistence.CDMCP_REMOTE_ROOT_DIR, "~/", "")) {
			matches := r.FindStringSubmatch(sshOut)
			// 3. Check if we found a match (and our capture group)
			if len(matches) < 2 {
				continue
			}

			// 4. The captured number is in matches[1] (as a string)
			numStr := matches[1]

			// 5. Convert the string to an integer
			CDMcpJobId, err := strconv.Atoi(numStr)
			if err != nil {
				genericCore.WriteToLog(fmt.Sprintf("Error converting matched string to integer:" + err.Error()))
				continue
			}
			genericCore.WriteToLog(fmt.Sprintf("Cluster Director MCP JobId CDMcpJobId : %d", CDMcpJobId))
			jobDataMap[CDMcpJobId] = slurmJobId
		} else {
			genericCore.WriteToLog("")
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Returning %d jobs \n", len(jobDataMap)))
	return jobDataMap, true, ""
}

func GetRunningSlurmJobsForUserInCluster(projectId string, clusterName string, zone string, loginNode string) ([]int, string, bool) {
	var runningJobIds []int

	currentUser, err := user.Current()
	if err != nil {
		return runningJobIds, "Could not get user-id (login name/LDAP)", false
	}

	sqCmd := "squeue -u $USER -t  RUNNING "
	sshOut, success := runSSHOnNode(loginNode, projectId, zone, sqCmd)
	if !success {
		genericCore.WriteToLog("GetRunningSlurmJobsForUserInCluster.3333")
		return runningJobIds, "Could not run squeue to get running jobs on " +
			loginNode + " in cluster " + clusterName + " in project " + projectId + "\n" + genericCore.GetLastLines(sshOut, 10), false
	}

	// Sample Output of : squeue -u $USER -t RUNNING
	// JOBID PARTITION     NAME     USER ST       TIME  NODES NODELIST(REASON)
	// 147     part1 build-nc ext_xxxx  R       1:47      1 xxxx-nodeset1-0
	if err != nil {
		return runningJobIds, "Could not get running jobs for user " + currentUser.Username + " using: squeue -u " + currentUser.Username + " -t RUNNING", false
	}

	var isNumericRegex = regexp.MustCompile(`^\d+$`)
	scanner := bufio.NewScanner(strings.NewReader(sshOut))
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)

		// 8 fields
		if len(fields) != 8 {
			continue // malformed due to wrong number of tokens
		}
		if isNumericRegex.MatchString(fields[0]) {
			jobId, err := strconv.Atoi(fields[0])
			if err != nil {
				genericCore.WriteToLog("Error converting " + fields[0] + " to integer")
				continue
			}
			runningJobIds = append(runningJobIds, jobId)
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Number of running jobs: %d", len(runningJobIds)))
	return runningJobIds, "", true
}

func GetMachineTypeForCluster(projectId string, clusterName string) []string {
	_, _ = getClustersInAllRegions(projectId)

	var machineTypes []string

	genericCore.WriteToLog("Trying to determine machine for cluster " + clusterName)
	for region := range MostRecentClusterData {
		genericCore.WriteToLog("Looking for cluster info in region region : " + region)
		for i := range MostRecentClusterData[region].Clusters {
			if clusterName != filepath.Base(MostRecentClusterData[region].Clusters[i].Name) {
				genericCore.WriteToLog("Ignoring because of name mismatch " + MostRecentClusterData[region].Clusters[i].Name)
				continue
			}
			genericCore.WriteToLog("Found cluster " + MostRecentClusterData[region].Clusters[i].Name)
			for j := range MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests {
				if MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests[j].MachineType != "" {
					machineTypes = append(machineTypes, MostRecentClusterData[region].Clusters[i].Compute.ResourceRequests[j].MachineType)
				}
			}
		}
	}

	genericCore.WriteToLog(fmt.Sprintf("Returning %d machineTypes", len(machineTypes)))
	return machineTypes
}
