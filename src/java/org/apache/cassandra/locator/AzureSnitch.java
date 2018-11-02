/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.locator;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A snitch that assumes an Azure region is a DC and an Azure (availability) zone
 *  is a rack. This information is available in the instance metadata for the VM.
 */
public class AzureSnitch extends AbstractNetworkTopologySnitch
{
    protected static final Logger logger = LoggerFactory.getLogger(AzureSnitch.class);

    protected static final String AZURE_QUERY_URL_TEMPLATE = "http://169.254.169.254/metadata/instance/compute/(%s)?api-version=2018-04-02&format=text";
    protected static final String REGION_NAME_QUERY_URL = String.format(AZURE_QUERY_URL_TEMPLATE, "location");
    protected static final String ZONE_NAME_QUERY_URL = String.format(AZURE_QUERY_URL_TEMPLATE, "zone");
    private static final String DEFAULT_DC = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";

    protected static final String MANAGEMENT_ENDPOINTS_QUERY_URL = "https://management.azure.com/metadata/endpoints?api-version=2018-07-01";

    final String azureRegion;
    private final String azureZone;

    private Map<InetAddressAndPort, Map<String, String>> savedEndpoints;

    public AzureSnitch() throws IOException, ConfigurationException
    {
        this(new SnitchProperties());
    }

    public AzureSnitch(SnitchProperties props) throws IOException, ConfigurationException
    {
        String region = azureApiCall(REGION_NAME_QUERY_URL);
        azureZone = azureApiCall(ZONE_NAME_QUERY_URL);

        String datacenterSuffix = props.get("dc_suffix", "");
        azureRegion = region.concat(datacenterSuffix);
        logger.info("AzureSnitch using region: {}, zone: {}.", azureRegion, azureZone);
    }

    String azureApiCall(String url) throws IOException, ConfigurationException
    {
        // Populate the region and zone by introspection, fail if 404 on metadata
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream d = null;
        try
        {
            // add required header
            conn.setRequestProperty("Metadata", "True");
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200)
                throw new ConfigurationException("AzureSnitch was unable to execute the API call. Not an Azure node?");

            // Read the information. I wish I could say (String) conn.getContent() here...
            int cl = conn.getContentLength();
            byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }

    public String getRack(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return azureZone;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.RACK) == null)
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("rack");
            return DEFAULT_RACK;
        }
        return state.getApplicationState(ApplicationState.RACK).value;
    }

    public String getDatacenter(InetAddressAndPort endpoint)
    {
        if (endpoint.equals(FBUtilities.getBroadcastAddressAndPort()))
            return azureRegion;
        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.DC) == null)
        {
            if (savedEndpoints == null)
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("data_center");
            return DEFAULT_DC;
        }
        return state.getApplicationState(ApplicationState.DC).value;
    }

    @Override
    public boolean validate(Set<String> datacenters, Set<String> racks)
    {

        String endpointMetadata;
        Set<String> locations;
        try
        {
            endpointMetadata = azureApiCall(MANAGEMENT_ENDPOINTS_QUERY_URL);
            locations = parseLocationsFromEndpointMetadata(endpointMetadata);
        }
        catch (IOException | ConfigurationException e)
        {
            logger.warn(e.getMessage());
            locations = new HashSet<>();
        }

        return validate(datacenters, racks, locations);
    }

    boolean validate(Set<String> datacenters, Set<String> racks, Set<String> locations)
    {
        // At this time, there does not seem to be any formal specification of what constitutes a valid
        // Azure region name, but by inspection, their structure appears to be very straightforward.

        // If we are able to retrieve a list of valid locations, we can check our datacenters against those.
        // But since we allow custom datacenter suffixes (CASSANDRA-5155) (and we can't make any assumptions
        // about that suffix by looking at this node's datacenterSuffix - conceivably there could be many
        // different suffixes in play for a given region), the best we can do is make sure the region name
        // starts with a valid location. If we don't have a list of locations, we just ensure the datacenter
        // follows the basic naming pattern, e.g. "eastus2<custom-suffix>" or "centralus<custom-suffix>".
        for (String dc : datacenters)
        {
            if (!locations.isEmpty()) {
                Boolean valid = false;
                for (String location : locations) {
                    if (dc.startsWith(location)) {
                        valid = true;
                    }
                }
                if (!valid) {
                    return false;
                }
            } else {
                if (!dc.matches("[a-z]+[\\d]?")) {
                    return false;
                }
            }
        }

        for (String rack : racks)
        {
            // At this time, Azure's availability zones are simply digits. Allow for the case that the
            // VM does not have an availability zone (represented by an empty string).
            if (!rack.isEmpty() && !rack.matches("[\\d]")) {
                return false;
            }
        }

        return true;
    }

    Set<String> parseLocationsFromEndpointMetadata(String endpointMetadataRaw) {
        Map<String, String> endpointMetadata = FBUtilities.fromJsonMap(endpointMetadataRaw);
        Map<String, String> allCloudEndpoints = FBUtilities.fromJsonMap(endpointMetadata.get("cloudEndpoint"));

        Set<String> locations = new HashSet<>();
        for (String endpoint : allCloudEndpoints.keySet()) {
            Map<String, String> endpointDetails = FBUtilities.fromJsonMap(endpoint);
            List<String> endpointLocations = FBUtilities.fromJsonList(endpointDetails.get("locations"));
            locations.addAll(endpointLocations);
        }
        return locations;
    }
}
