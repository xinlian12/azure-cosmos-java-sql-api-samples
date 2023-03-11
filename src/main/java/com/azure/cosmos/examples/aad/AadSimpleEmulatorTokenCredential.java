// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.examples.aad;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.cosmos.implementation.Utils;
import reactor.core.publisher.Mono;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

public class AadSimpleEmulatorTokenCredential implements TokenCredential {
    private final String emulatorKeyEncoded;
    private final String AAD_HEADER_COSMOS_EMULATOR = "{\"typ\":\"JWT\",\"alg\":\"RS256\",\"x5t\":\"CosmosEmulatorPrimaryMaster\",\"kid\":\"CosmosEmulatorPrimaryMaster\"}";
    private final String AAD_CLAIM_COSMOS_EMULATOR_FORMAT = "{\"aud\":\"https://localhost.localhost\",\"iss\":\"https://sts.fake-issuer.net/7b1999a1-dfd7-440e-8204-00170979b984\",\"iat\":%d,\"nbf\":%d,\"exp\":%d,\"aio\":\"\",\"appid\":\"localhost\",\"appidacr\":\"1\",\"idp\":\"https://localhost:8081/\",\"oid\":\"96313034-4739-43cb-93cd-74193adbe5b6\",\"rh\":\"\",\"sub\":\"localhost\",\"tid\":\"EmulatorFederation\",\"uti\":\"\",\"ver\":\"1.0\",\"scp\":\"user_impersonation\",\"groups\":[\"7ce1d003-4cb3-4879-b7c5-74062a35c66e\",\"e99ff30c-c229-4c67-ab29-30a6aebc3e58\",\"5549bb62-c77b-4305-bda9-9ec66b85d9e4\",\"c44fd685-5c58-452c-aaf7-13ce75184f65\",\"be895215-eab5-43b7-9536-9ef8fe130330\"]}";

    public AadSimpleEmulatorTokenCredential(String emulatorKey) {
        if (emulatorKey == null || emulatorKey.isEmpty()) {
            throw new IllegalArgumentException("emulatorKey");
        }

        this.emulatorKeyEncoded = Utils.encodeUrlBase64String(emulatorKey.getBytes());
    }

    @Override
    public Mono<AccessToken> getToken(TokenRequestContext tokenRequestContext) {
        String aadToken = emulatorKey_based_AAD_String();
        return Mono.just(new AccessToken(aadToken, OffsetDateTime.now().plusHours(2)));
    }

    String emulatorKey_based_AAD_String() {
        ZonedDateTime currentTime = ZonedDateTime.now();
        String part1Encoded = Utils.encodeUrlBase64String(AAD_HEADER_COSMOS_EMULATOR.getBytes());
        String part2 = String.format(AAD_CLAIM_COSMOS_EMULATOR_FORMAT,
                currentTime.toEpochSecond(),
                currentTime.toEpochSecond(),
                currentTime.plusHours(2).toEpochSecond());
        String part2Encoded = Utils.encodeUrlBase64String(part2.getBytes());
        return part1Encoded + "." + part2Encoded + "." + this.emulatorKeyEncoded;
    }
}
