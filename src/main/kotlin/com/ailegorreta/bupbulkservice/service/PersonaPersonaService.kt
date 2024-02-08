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
 *  PersonaPersonaService.kt
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
 * Service event consumer for relationship between Persons
 *
 * The relationship types are:
 * - RELACION
 *
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class PersonaPersonaService(@Qualifier("client_credentials") val webClient: WebClient,
                              private val personaService: PersonaService,
                              private val eventService: EventService,
                              private val serviceConfig: ServiceConfig,
                              private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * persons
     */
    fun processEvent(event: String): String {

        try {
            val personaPersona = mapper.readValue(event, PersonaPersona::class.java)

            logger.info("Received message from telephone:$personaPersona")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val personsFrom = personaService.getPerson(personaPersona.idPersonaDe)
                val personsTo = personaService.getPerson(personaPersona.idPersonaA)

                if (personsFrom.size == 1 && personsTo.size == 1) {
                    if (personaPersona.relacion == "RELACION") {
                        addRelacion(fromId = personsFrom.first().idNeo4j!!, toId = personsTo.first().idNeo4j!!,
                                    tipo = personaPersona.extraData1, name = personaPersona.extraData2)
                    } else {
                        logger.error("Error al añadir relación no existe ${personaPersona.relacion} ")

                        eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                            headers = null, userName = "ingestor",
                            eventName = "ERROR:NO_EXISTE", value = personaPersona)
                        throw Exception("Error al leer añadir relación persona con persona")
                    }
                } else {
                    logger.error("Error al añadir leer persona ${personaPersona.idPersonaDe} y  ${personaPersona.idPersonaA}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:RELACION_RELACION", value = personaPersona)
                    throw Exception("Error al añadir la relación entre persona y persona")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addRelacion(fromId: String, toId: String, tipo: String, name: String): Relacion? {
        val addRelationshipGraphql = """
            mutation newRelacion(${'$'}from: ID!, ${'$'}tipo: String!, ${'$'}nombre: String!, ${'$'}to: ID!) {
                createRelacion (from__id: ${'$'}from, tipo: ${'$'}tipo, nombre: ${'$'}nombre, to__id:${'$'}to) {
                    tipo
                    nombre
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addRelationshipGraphql,
                                                    mutableMapOf("from" to fromId,
                                                        "to" to toId,
                                                        "tipo" to tipo,
                                                        "nombre" to name))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateRelacion::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un una relación entre personas:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("fromId" to fromId))

            graphQLError.addExtraData("toId", toId)
            graphQLError.addExtraData("tipo", tipo)
            graphQLError.addExtraData("name", name)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_RELACION_PERSONA", value = graphQLError)
            return null
        }

        return res.body!!.data!!.createRelacion
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class PersonaPersona(val idPersonaDe: Int,
                              val idPersonaA: Int,
                              val relacion: String,
                              val extraData1: String,
                              val extraData2: String)

    data class Relacion (var to: PersonaService.Persona? = null,
                         var tipo: String,
                         var nombre: String)

    data class GraphqlResponseCreateRelacion(val data: Data? = null,
                                             val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createRelacion: Relacion)
    }
}