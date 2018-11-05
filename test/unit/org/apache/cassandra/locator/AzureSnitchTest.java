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


import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;

public class AzureSnitchTest
{
    private static String region;
    private static String zone;

    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.daemonInitialization();
        SchemaLoader.mkdirs();
        SchemaLoader.cleanup();
        Keyspace.setInitialized();
        StorageService.instance.initServer(0);
    }

    private static class TestAzureSnitch extends AzureSnitch
    {
        public TestAzureSnitch() throws IOException, ConfigurationException
        {
            super();
        }

        public TestAzureSnitch(SnitchProperties props) throws IOException, ConfigurationException
        {
            super(props);
        }

        @Override
        String azureApiCall(String url) throws IOException, ConfigurationException
        {
            if (url.contains("metadata/instance/compute/location")) {
                // region
                return region;
            } else if (url.contains("metadata/instance/compute/zone")) {
                // zone
                return zone;
            } else if (url.contains("metadata/endpoints")) {
                // subset of actual endpoint metadata (as of Nov 2018)
                return "{\"cloudEndpoint\":{"
                       + "\"public\":{\"endpoint\":\"management.azure.com\",\"locations\":[\"westus\",\"westus2\",\"eastus\",\"centralus\",\"centraluseuap\",\"southcentralus\",\"northcentralus\",\"westcentralus\",\"eastus2\",\"eastus2euap\","
                       + "\"brazilsouth\",\"brazilus\",\"northeurope\",\"westeurope\",\"eastasia\",\"southeastasia\",\"japanwest\",\"japaneast\",\"koreacentral\",\"koreasouth\",\"indiasouth\",\"indiawest\",\"indiacentral\","
                       + "\"australiaeast\",\"australiasoutheast\",\"canadacentral\",\"canadaeast\",\"uknorth\",\"uksouth2\",\"uksouth\",\"ukwest\",\"francecentral\",\"francesouth\",\"australiacentral\",\"australiacentral2\"]},"
                       + "\"chinaCloud\":{\"endpoint\":\"management.chinacloudapi.cn\",\"locations\":[\"chinaeast\",\"chinanorth\",\"chinanorth2\",\"chinaeast2\"]},"
                       + "\"usGovCloud\":{\"endpoint\":\"management.usgovcloudapi.net\",\"locations\":[\"usgovvirginia\",\"usgoviowa\",\"usdodeast\",\"usdodcentral\",\"usgovtexas\",\"usgovarizona\"]},"
                       + "\"germanCloud\":{\"endpoint\":\"management.microsoftazure.de\",\"locations\":[\"germanycentral\",\"germanynortheast\"]}}}";
            }
            return "";
        }
    }

    AzureSnitch getSnitchForValidation() throws IOException, ConfigurationException
    {
        region = "foo-not-tested";
        zone = "foo-not-tested";
        return new TestAzureSnitch();
    }

    @Test
    public void testFullNamingScheme() throws IOException, ConfigurationException
    {
        region = "eastus";
        zone = "1";

        InetAddressAndPort local = InetAddressAndPort.getByName("127.0.0.1");
        AzureSnitch snitch = new TestAzureSnitch();

        assertEquals("eastus", snitch.getDatacenter(local));
        assertEquals("1", snitch.getRack(local));

        region = "westus2";
        zone = "3";
        snitch = new TestAzureSnitch();

        assertEquals("westus2", snitch.getDatacenter(local));
        assertEquals("3", snitch.getRack(local));
    }

    @Test
    public void validateRacks_InvalidName() throws IOException, ConfigurationException
    {
        AzureSnitch snitch = getSnitchForValidation();
        region = "foo-not-tested";
        zone = "foo-not-tested";
        Set<String> racks = new HashSet<>();
        racks.add("1a");
        Assert.assertFalse(snitch.validate(Collections.emptySet(), racks));
    }

    @Test
    public void validateRacks_ValidNames() throws IOException, ConfigurationException
    {
        AzureSnitch snitch = getSnitchForValidation();
        Set<String> racks = new HashSet<>();
        racks.add("2");
        racks.add("3");
        racks.add("");
        Assert.assertTrue(snitch.validate(Collections.emptySet(), racks));
    }

    @Test
    public void validate_HappyPath() throws IOException, ConfigurationException
    {
        AzureSnitch snitch = getSnitchForValidation();
        Set<String> datacenters = new HashSet<>();
        datacenters.add("eastus2");
        Set<String> racks = new HashSet<>();
        racks.add("1");
        Assert.assertTrue(snitch.validate(datacenters, racks));
    }

    @Test
    public void validate_HappyPathWithDCSuffix() throws IOException, ConfigurationException
    {
        AzureSnitch snitch = getSnitchForValidation();
        Set<String> datacenters = new HashSet<>();
        datacenters.add("eastus2_CUSTOM_SUFFIX");
        Set<String> racks = new HashSet<>();
        racks.add("1");
        Assert.assertTrue(snitch.validate(datacenters, racks));
    }

    @AfterClass
    public static void tearDown()
    {
        StorageService.instance.stopClient();
    }
}
