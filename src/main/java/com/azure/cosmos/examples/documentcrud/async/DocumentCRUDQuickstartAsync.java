// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.documentcrud.async;

import com.azure.cosmos.*;
import com.azure.cosmos.examples.common.AccountSettings;
import com.azure.cosmos.examples.common.Families;
import com.azure.cosmos.examples.common.Family;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseRequestOptions;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DocumentCRUDQuickstartAsync {

    private CosmosAsyncClient client;

    private final String databaseName = "AzureSampleFamilyDB";
    private final String containerName = "FamilyContainer";
    private final String documentId = UUID.randomUUID().toString();
    private final String documentLastName = "Witherspoon";

    private CosmosAsyncDatabase database;
    private CosmosAsyncContainer container;

    private AtomicBoolean replaceDone = new AtomicBoolean(false);
    private AtomicBoolean upsertDone = new AtomicBoolean(false);

    private AtomicBoolean createDocDone = new AtomicBoolean(false);
    private AtomicBoolean createDocsDone = new AtomicBoolean(false);

    private AtomicBoolean readDocDone = new AtomicBoolean(false);

    private List<JsonNode> jsonList;
    private String etag1;
    private String etag2;

    private AtomicBoolean updateEtagDone = new AtomicBoolean(false);
    private AtomicBoolean secondUpdateDone = new AtomicBoolean(false);

    private AtomicBoolean reReadDone = new AtomicBoolean(false);

    Family family = new Family();
    Family family2 = new Family();

    private AtomicBoolean isFamily2Updated = new AtomicBoolean(false);


    protected static Logger logger = LoggerFactory.getLogger(DocumentCRUDQuickstartAsync.class);

    public void close() {
        client.close();
    }

    /**
     * Sample to demonstrate the following document CRUD operations:
     * -Create
     * -Read by ID
     * -Read all
     * -Query
     * -Replace
     * -Upsert
     * -Replace with conditional ETag check
     * -Read document only if document has changed
     * -Delete
     */
    public static void main(String[] args) {
        DocumentCRUDQuickstartAsync p = new DocumentCRUDQuickstartAsync();
        try {
            logger.info("Starting ASYNC main");
            p.documentCRUDDemo();
            logger.info("Demo complete, please hold while resources are released");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("Cosmos getStarted failed with %s", e));
        } finally {
            logger.info("Closing the client");
            p.shutdown();
        }
    }

    private void documentCRUDDemo() throws Exception {

        logger.info("Using Azure Cosmos DB endpoint: " + AccountSettings.HOST);

        //  Create sync client
        client = new CosmosClientBuilder()
                .endpoint(AccountSettings.HOST)
                .key(AccountSettings.MASTER_KEY)
                .consistencyLevel(ConsistencyLevel.EVENTUAL)
                .contentResponseOnWriteEnabled(true)
                .buildAsyncClient();


        createDatabaseIfNotExists();
        createContainerIfNotExists();

        //add docs to array
        List<Family> families = new ArrayList<>();
        families.add(Families.getAndersenFamilyItem());
        families.add(Families.getWakefieldFamilyItem());
        families.add(Families.getJohnsonFamilyItem());
        families.add(Families.getSmithFamilyItem());

        // add array of docs to reactive stream
        Flux<Family> familiesToCreate = Flux.fromIterable(families);

        //this will insert a hardcoded doc
        createDocument();

        //this will insert the docs in the reactive stream
        createDocuments(familiesToCreate);

        while (!(createDocDone.get() && createDocsDone.get())) {
            //waiting for async createDoc and createDocs to complete...
            logger.info("waiting for async createDoc and createDocs to complete...");
            Thread.sleep(100);
        }

        //done async
        readDocumentById();
        //readAllDocumentsInContainer(); <Deprecated>

        //done async
        queryDocuments();

        getDocumentsAsJsonArray();
        while (this.jsonList == null) {
            //wait for jsonList to be set
            logger.info("waiting in while (this.jsonList == null) {");
            Thread.sleep(100);
        }
        //convert jsonList to ArrayNode
        ArrayNode jsonArray = new ArrayNode(JsonNodeFactory.instance, this.jsonList);
        logger.info("docs as json array: " + jsonArray.toString());
        replaceDocument();
        upsertDocument();
        while (!(upsertDone.get() && replaceDone.get())) {
            logger.info("waiting for async upsert and replace to complete...");
            Thread.sleep(100);
        }
        logger.info("replace and upsert done now...");
        replaceDocumentWithConditionalEtagCheck();
        readDocumentOnlyIfChanged();
        // deleteDocument() is called at shutdown()

    }

    // Database Create
    private void createDatabaseIfNotExists() throws Exception {
        logger.info("Create database " + databaseName + " if not exists...");

        //  Create database if not exists
        CosmosDatabaseResponse databaseResponse = client.createDatabaseIfNotExists(databaseName).block();
        database = client.getDatabase(databaseResponse.getProperties().getId());

        logger.info("Done.");
    }

    // Container create
    private void createContainerIfNotExists() throws Exception {
        logger.info("Create container " + containerName + " if not exists.");

        // Create container if not exists - this is async but we block to make sure
        // database and containers are created before sample runs the CRUD operations on
        // them
        CosmosContainerProperties containerProperties =
                new CosmosContainerProperties(containerName, "/lastName");

        // Provision throughput
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);

        //  Create container with 200 RU/s
        CosmosContainerResponse containerResponse = database.createContainerIfNotExists(containerProperties, throughputProperties).block();
        container = database.getContainer(containerResponse.getProperties().getId());

        logger.info("Done.");
    }

    private void createDocument() throws Exception {
        logger.info("Create document " + documentId);

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)

        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);

        // Insert this item as a document
        // Explicitly specifying the /pk value improves performance.
        // add subscribe() to make this async
        container.createItem(family, new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess((response) -> {
                    logger.info("inserted doc with id: " + response.getItem().getId());
                    createDocDone.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("creating doc asynchronously...");
    }

    private void createDocuments(Flux<Family> families) throws Exception {
        logger.info("Create documents " + documentId);

        // Define a document as a POJO (internally this
        // is converted to JSON via custom serialization)
        // Combine multiple item inserts, associated success println's, and a final
        // aggregate stats println into one Reactive stream.
        families.flatMap(family -> {
                    return container.createItem(family);
                }) // Flux of item request responses
                .flatMap(itemResponse -> {
                    logger.info(String.format("Created item with request charge of %.2f within" +
                                    " duration %s",
                            itemResponse.getRequestCharge(), itemResponse.getDuration()));
                    logger.info(String.format("Item ID: %s\n", itemResponse.getItem().getId()));
                    return Mono.just(itemResponse.getRequestCharge());
                }) // Flux of request charges
                .reduce(0.0,
                        (charge_n, charge_nplus1) -> charge_n + charge_nplus1) // Mono of total charge - there will be
                // only one item in this stream
                .doOnSuccess(itemResponse -> {
                    logger.info("Aggregated charge (all decimals): " + itemResponse);
                    // Preserve the total charge and print aggregate charge/item count stats.
                    logger.info(String.format("Created items with total request charge of %.2f\n", itemResponse));
                    createDocsDone.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                })
                .subscribe();
    }

    // Document read
    private void readDocumentById() throws Exception {
        logger.info("Read document " + documentId + " by ID.");

        //  Read document by ID
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess((response) -> {
                    try {
                        logger.info("item converted to json: " + new ObjectMapper().writeValueAsString(response.getItem()));
                        logger.info("Finished reading family " + response.getItem().getId() + " with partition key " + response.getItem().getLastName());


                    } catch (JsonProcessingException e) {
                        logger.error("Exception processing json: " + e);
                    }
                })
                .doOnError(Exception.class, exception -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        logger.info("readDocumentById done asynchronously...");
    }

    private void queryDocuments() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");

        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";

        CosmosPagedFlux<Family> filteredFamilies = container.queryItems(sql, new CosmosQueryRequestOptions(), Family.class);

        // Print
        filteredFamilies.byPage(100).flatMap(filteredFamiliesFeedResponse -> {
            logger.info("Got a page of query result with " +
                    filteredFamiliesFeedResponse.getResults().size() + " items(s)"
                    + " and request charge of " + filteredFamiliesFeedResponse.getRequestCharge());
            for (Family family : filteredFamiliesFeedResponse.getResults()) {
                logger.info("First query result: Family with (/id, partition key) = (%s,%s)", family.getId(), family.getLastName());
            }

            return Flux.empty();
        }).subscribe();
        logger.info("queryDocuments done asynchronously.");
    }

    private void getDocumentsAsJsonArray() throws Exception {
        logger.info("Query documents in the container " + containerName + ".");
        int preferredPageSize = 10;
        String sql = "SELECT * FROM c WHERE c.lastName = 'Witherspoon'";
        CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
        queryOptions.setQueryMetricsEnabled(true);
        CosmosPagedFlux<JsonNode> pagedFlux = container.queryItems(sql, queryOptions,
                JsonNode.class);
        pagedFlux.byPage(preferredPageSize)
                .flatMap(pagedFluxResponse -> {
                    // collect all documents in reactive stream to a list of JsonNodes
                    return Flux.just(pagedFluxResponse
                            .getResults()
                            .stream()
                            .collect(Collectors.toList()));
                })
                .onErrorResume((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                    return Mono.empty();
                }).subscribe(listJsonNode -> {
                    //pass the List<JsonNode> to the instance variable
                    this.jsonList = listJsonNode;
                });

    }

    private void replaceDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        container.replaceItem(family, family.getId(),
                        new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                    //set flag so calling method can know when done
                    replaceDone.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("replace subscribed: will process asynchronously....");
    }

    private void upsertDocument() throws Exception {
        logger.info("Replace document " + documentId);

        // Replace existing document with new modified document (contingent on
        // modification).
        Family family = new Family();
        family.setLastName(documentLastName);
        family.setId(documentId);
        family.setDistrict("Columbia"); // Document modification

        container.upsertItem(family, new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of upsert operation: {} RU", itemResponse.getRequestCharge());
                    // set flag so calling method can know when done
                    upsertDone.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();
        logger.info("upsert subscribed: will process asynchronously....");
    }

    private void replaceDocumentWithConditionalEtagCheck() throws Exception {
        logger.info("Replace document " + documentId + ", employing optimistic concurrency using ETag.");

        // Obtained current document ETag
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of readItem operation: {} RU", itemResponse.getRequestCharge());
                    this.family = itemResponse.getItem();
                    this.etag1 = itemResponse.getETag();
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception 1. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        while (this.etag1 == null) {
            logger.info("waiting until we got the etag1 from the first read....");
            Thread.sleep(100);
        }
        String etag = this.etag1;
        logger.info("Read document " + documentId + " to obtain current ETag: " + etag);

        // Modify document
        Family family = this.family;
        family.setRegistered(!family.isRegistered());

        // Persist the change back to the server, updating the ETag in the process
        // This models a concurrent change made to the document
        container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("'Concurrent' update to document " + documentId + " so ETag is now " + itemResponse.getResponseHeaders().get("etag"));
                    updateEtagDone.set(true);
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception 2. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();


        while (updateEtagDone.get() == false) {
            //wait until update done
        }
        // Now update the document and call replace with the AccessCondition requiring that ETag has not changed.
        // This should fail because the "concurrent" document change updated the ETag.

        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfMatchETag(etag);

        family.setDistrict("Seafood");

        container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), requestOptions)
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.info("As expected, we have a pre-condition failure exception\n");
                })
                .onErrorResume(exception -> Mono.empty())
                .subscribe();
    }

    private void readDocumentOnlyIfChanged() throws Exception {
        logger.info("Read document " + documentId + " only if it has been changed, utilizing an ETag check.");

        // Read document
        container.readItem(documentId, new PartitionKey(documentLastName), Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Read doc with status code of {}", itemResponse.getStatusCode());
                    //assign etag to the instance variable asynchronously
                    this.family2 = itemResponse.getItem();
                    this.isFamily2Updated.set(true);
                    this.etag2 = itemResponse.getResponseHeaders().get("etag");
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        while (this.etag2 == null) {
            logger.info("waiting until we got the etag2 from the first read....");
            Thread.sleep(100);
        }
        while (this.isFamily2Updated.get() == false) {
            //wait for family to be upadted...
            logger.info("waiting until family2 got updated....");
            Thread.sleep(100);
        }
        ;
        //etag retrieved from first read so we can safely assign to instance variable
        String etag = this.etag2;

        // Re-read doc but with conditional access requirement that ETag has changed.
        // This should fail.
        CosmosItemRequestOptions requestOptions = new CosmosItemRequestOptions();
        requestOptions.setIfNoneMatchETag(etag);

        container
                .readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class)
                .doOnSuccess(itemResponse -> {
                    reReadDone.set(true);
                    logger.info("Re-read doc with status code of {} (we anticipate failure due to ETag not having changed.)", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.info("As expected, we have a second pre-condition failure exception\n");
                })
                .onErrorResume(exception -> Mono.empty())
                .subscribe();

        while (reReadDone.get() == false) {
            //wait for reRead
            Thread.sleep(100);
        }
        // Replace the doc with a modified version, which will update ETag
        Family family = this.family2;
        family.setRegistered(!family.isRegistered());
        container.replaceItem(family, family.getId(), new PartitionKey(family.getLastName()), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    secondUpdateDone.set(true);
                    logger.info("Request charge of replace operation: {} RU", itemResponse.getRequestCharge());
                    logger.info("Modified and replaced the doc (updates ETag.)");
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        while (secondUpdateDone.get() == false) {
            //wait for second update before re-reading the doc...
        }

        // Re-read doc again, with conditional access requirements.
        // This should succeed since ETag has been updated.
        container.readItem(documentId, new PartitionKey(documentLastName), requestOptions, Family.class)
                .doOnSuccess(itemResponse -> {
                    logger.info("Re-read doc with status code of {} (we anticipate success due to ETag modification.)", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).subscribe();

        logger.info("final etag check will be done async...");
    }

    // Document delete
    private void deleteADocument() throws Exception {
        logger.info("Delete document " + documentId + " by ID.");

        // Delete document
        container.deleteItem(documentId, new PartitionKey(documentLastName), new CosmosItemRequestOptions())
                .doOnSuccess(itemResponse -> {
                    logger.info("Request charge of delete document operation: {} RU", itemResponse.getRequestCharge());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();

        logger.info("Done.");
    }

    // Database delete
    private void deleteADatabase() throws Exception {
        logger.info("Last step: delete database " + databaseName + " by ID.");

        // Delete database
        client.getDatabase(databaseName).delete(new CosmosDatabaseRequestOptions()).doOnSuccess(itemResponse -> {
                    logger.info("Request charge of delete database operation: {} RU", itemResponse.getRequestCharge());
                    logger.info("Status code for database delete: {}", itemResponse.getStatusCode());
                })
                .doOnError((exception) -> {
                    logger.error(
                            "Exception. e: {}",
                            exception.getLocalizedMessage(),
                            exception);
                }).block();

        logger.info("Done.");
    }

    // Cleanup before close
    private void shutdown() {
        try {
            //Clean shutdown
            deleteADocument();
            deleteADatabase();
        } catch (Exception err) {
            logger.error("Deleting Cosmos DB resources failed, will still attempt to close the client. See stack trace below.");
            err.printStackTrace();
        }
        client.close();
        logger.info("Done with sample.");
    }

}
