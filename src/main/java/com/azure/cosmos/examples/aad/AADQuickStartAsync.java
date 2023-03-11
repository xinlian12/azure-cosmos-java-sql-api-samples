// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.aad;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.identity.ClientSecretCredentialBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class AADQuickStartAsync {
    private static final Logger logger = LoggerFactory.getLogger(com.azure.cosmos.examples.aad.AADQuickStartAsync.class);

    public static void main(String[] args) {

        String tenantId="72f988bf-86f1-41af-91ab-2d7cd011db47";
        String clientId = "clientId";
        String clientSecret = "clientSecret";
        String endpoint = "https://oltp-spark-ci-sql.documents.azure.com:443/";
        String containerName = "AADTestContainer";
        String databaseName = "AADTestDatabase";

        TokenCredential tokenCredential= new ClientSecretCredentialBuilder()
                .authorityHost("https://login.microsoftonline.com")
                .tenantId(tenantId)
                .clientId(clientId)
                .clientSecret(clientSecret)
                .build();

        CosmosAsyncClient client=new CosmosClientBuilder()
                .endpoint(endpoint)
                .credential(tokenCredential)
                .buildAsyncClient();

        // read database
        client.getDatabase(databaseName).read()
                .map(databaseResponse -> {
                    logger.info("Getting database response successfully. CosmosDiagnostics {}", databaseResponse.getDiagnostics());
                    return databaseResponse;
                })
                .block();

        // read container
        client.getDatabase(databaseName).getContainer(containerName).read()
                .map(containerResponse -> {
                    logger.info("Getting container response successfully. CosmosDiagnostics {}", containerResponse.getDiagnostics());
                    return containerResponse;
                })
                .block();

        // read container throughput
        client.getDatabase(databaseName).getContainer(containerName).readThroughput()
                .map(throughputResponse -> {
                    logger.info("Getting throughput response successfully. CosmosDiagnostics {}", throughputResponse.getDiagnostics());
                    return throughputResponse;
                })
                .onErrorResume(throwable -> {
                    logger.error("Getting throughput response failed. CosmosDiagnostics {}", ((CosmosException)throwable).getDiagnostics());
                    return Mono.empty();
                })
                .block();
    }
}
