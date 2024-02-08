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
 *  CompaniaCompaniaService.kt
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
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.errors.SerializationException
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.http.MediaType
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import java.util.concurrent.CountDownLatch

/**
 * Service event consumer for relationship between Companies
 *
 * The relationship types are:
 * - PROVEEDOR
 *
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class CompaniaCompaniaService(@Qualifier("client_credentials") val webClient: WebClient,
                             private val companiaService: CompaniaService,
                             private val eventService: EventService,
                             private val serviceConfig: ServiceConfig,
                             private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * companies
     */
    fun processEvent(event: String): String {

        try {
            val companiaCompania = mapper.readValue(event, CompaniaCompania::class.java)

            logger.info("Received message from telephone:$companiaCompania")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val companiesFrom = companiaService.getCompany(companiaCompania.idCompaniaDe)
                val companiesTo = companiaService.getCompany(idPersona = companiaCompania.idCompaniaA)

                if (companiesFrom.size == 1 && companiesTo.size == 1) {
                    if (companiaCompania.relacion == "PROVEEDOR") {
                        addProveedor(fromId = companiesFrom.first().idNeo4j!!, toId = companiesTo.first().idNeo4j!!,
                                     tipo = companiaCompania.extraData)
                    } else {
                        logger.error("Error al añadir relación no existe ${companiaCompania.relacion} ")

                        eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                            headers = null, userName = "ingestor",
                            eventName = "ERROR:NO_EXISTE", value = companiaCompania)
                        throw Exception("Error al leer añadir relación compañía con compañía")
                    }
                } else {
                    logger.error("Error al añadir leer compañía ${companiaCompania.idCompaniaDe} y  ${companiaCompania.idCompaniaA}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:RELACION_PROVEEDOR", value = companiaCompania)
                    throw Exception("Error al añadir la relación entre compañía y compañía de proveedor")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addProveedor(fromId: String, toId: String, tipo: String): Proveedor? {
        val addSupplierGraphql = """
            mutation newProveedor(${'$'}from: ID!, ${'$'}tipo: String!, ${'$'}to: ID!) {
                createProveedor (from__id: ${'$'}from, tipo: ${'$'}tipo, to__id:${'$'}to) {
                    tipo
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addSupplierGraphql,
                                                        mutableMapOf("from" to fromId,
                                                            "to" to toId,
                                                            "tipo" to tipo))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateProveedor::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un proveedor a la compañía:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("fromId" to fromId))

            graphQLError.addExtraData("toId", toId)
            graphQLError.addExtraData("tipo", tipo)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_PROVEEDOR_COMPAÑIA", value = graphQLError)
            return null
        }

        return res.body!!.data!!.createProveedor
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class CompaniaCompania(val idCompaniaDe: Int,
                                val idCompaniaA: Int,
                                val relacion: String,
                                val extraData: String)

    data class Proveedor(var to: CompaniaService.Compania? = null,
                         var tipo: String)

    data class GraphqlResponseCreateProveedor(val data: Data? = null,
                                              val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createProveedor: Proveedor)
    }
}