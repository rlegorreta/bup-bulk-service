/* Copyright (c) 2024, LegoSoft Soluciones, S.C.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are not permitted.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 *  RfcService.kt
 *
 *  Developed 2024 by LegoSoftSoluciones, S.C. www.legosoft.com.mx
 */
package com.ailegorreta.bupbulkservice.service

import com.ailegorreta.bupbulkservice.config.ServiceConfig
import com.ailegorreta.bupbulkservice.data.GraphqlRequestBody
import com.ailegorreta.commons.event.EventGraphqlError
import com.ailegorreta.commons.event.EventType
import com.ailegorreta.commons.utils.HasLogger
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import java.util.concurrent.CountDownLatch

/**
 * Service that adds the rfc relationship for Persons and Companies.
 *
 * If the Rfc does not exist we insert it
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class RfcService(@Qualifier("client_credentials") val webClient: WebClient,
                 private val eventService: EventService,
                 private val serviceConfig: ServiceConfig): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())


    private fun getRfc(rfc: String): List<Rfc>? {
        val variables = mutableMapOf("rfc" to rfc)
        val getRfcGraphql = """
            query getRfc(${'$'}rfc: String) {
                rfcs(rfc: ${'$'}rfc) {
                    _id
                    rfc
                }
            }            
        """.trimIndent()

        val graphQLRequestBody = GraphqlRequestBody(getRfcGraphql,variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseRfcs::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer Rfc:" + res?.errors)
            return null
        }

        return res.data!!.rfcs
    }

    fun addRfcIfNotExists(rfc: String): Rfc? {
        // check if not exist the Rfc
        val rfcs = getRfc(rfc = rfc) ?: return null

        if (rfcs.isNotEmpty()) return rfcs.first()

        val addRfcGraphql = """
            mutation newRfc(${'$'}rfc: String!) {
                createRfc (rfc: ${'$'}rfc) {
                    _id
                    rfc
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addRfcGraphql, mutableMapOf("rfc" to rfc))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateRfc::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un rfc:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_RFC", value = EventGraphqlError(res.body?.errors, mutableMapOf("rfc" to rfc))
            )
            return null
        }

        return res.body!!.data!!.createRfc
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Rfc(var _id: String? = null,
                   var rfc: String? = null) {

        var idNeo4j = _id       // this is because the mapping done from the endpoint with '_id' it ignores de underscore
        // and make it wrong
    }

    data class GraphqlResponseRfcs(val data: Data? = null,
                                   val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val rfcs: List<Rfc>)
    }

    data class GraphqlResponseCreateRfc(val data: Data? = null,
                                        val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createRfc: Rfc)
    }
}