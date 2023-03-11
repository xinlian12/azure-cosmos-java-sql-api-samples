// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.aad;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class AADWithEmulatorQuickStartAsync {
    private static final Logger logger = LoggerFactory.getLogger(com.azure.cosmos.examples.aad.AADQuickStartAsync.class);

    public static void main(String[] args) {
        String emulatorKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
        String emulatorHost = "https://localhost:8081/";
        String containerName = "AADTestContainer";
        String databaseName = "AADTestDatabase";

        TokenCredential emulatorCredential = new AadSimpleEmulatorTokenCredential(emulatorKey); // Cosmos public emulator key
        CosmosAsyncClient client = new CosmosClientBuilder()
                .endpoint(emulatorHost) // Cosmos public emulator endpoint
                .credential(emulatorCredential)
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
