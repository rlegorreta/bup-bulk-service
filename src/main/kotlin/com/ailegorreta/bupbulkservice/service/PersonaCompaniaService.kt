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
 *  PersonaCompaniaService.kt
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
 * Service event consumer for relationship between Persons and Companies
 *
 * The relationship types are:
 * - TRABAJA
 * - DIRIGE : in fact this is not with companies but with company areas
 *
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class PersonaCompaniaService(@Qualifier("client_credentials") val webClient: WebClient,
                              private val personaService: PersonaService,
                              private val companiaService: CompaniaService,
                              private val areaService: AreaService,
                              private val eventService: EventService,
                              private val serviceConfig: ServiceConfig,
                              private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * person and the company
     */
    fun processEvent(event: String): String {

        try {
            val personaCompania = mapper.readValue(event, PersonaCompania::class.java)

            logger.info("Received message from telephone:$personaCompania")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val persons = personaService.getPerson(personaCompania.idPersona)
                val companies = companiaService.getCompany(idPersona = personaCompania.idCompania)

                if (persons.size == 1 && companies.size == 1) {
                    if (personaCompania.relacion == "TRABAJA") {
                        addWork(fromId = persons.first().idNeo4j!!, toId = companies.first().idNeo4j!!,
                                puesto = personaCompania.extraData)
                    } else if (personaCompania.relacion == "DIRIGE") {
                        val area = areaService.addAreaIfNotExists(personaCompania.extraData)

                        if (area != null)
                            addDirects(idPersona = persons.first().idNeo4j!!, toIdArea = area.idNeo4j!!,
                                       idCompania = companies.first().idNeo4j!!, nombreCompania = companies.first().nombre)
                        else {
                            logger.error("Error al añadir leer area ${personaCompania.idPersona} y area ${personaCompania.extraData}:")

                            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                headers = null, userName = "ingestor",
                                eventName = "ERROR:RELACION_DIRIGE", value = personaCompania)
                            throw Exception("Error al leer añadir un área")
                        }
                    } else {
                        logger.error("Error al añadir relación no existe ${personaCompania.extraData} ")

                        eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                                headers = null, userName = "ingestor",
                                                eventName = "ERROR:NO_EXISTE", value = personaCompania)
                        throw Exception("Error al leer añadir relación persona compañía")
                    }
                } else {
                    logger.error("Error al añadir leer persona ${personaCompania.idPersona} y idCompania ${personaCompania.idCompania}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:RELACION_TRABAJA_DIRIGE", value = personaCompania)
                    throw Exception("Error al añadir la relación entrea persona y compañía de trabaja o dirige")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addWork(fromId: String, toId: String, puesto: String): Trabaja? {
        val addWorkGraphql = """
             mutation newTrabaja(${'$'}from: ID!, ${'$'}puesto: String!, ${'$'}to: ID!) {
                createTrabaja (from__id: ${'$'}from, puesto: ${'$'}puesto, to__id:${'$'}to) {
                    puesto
                }
            }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addWorkGraphql,
                                            mutableMapOf("from" to fromId,
                                                         "to" to toId,
                                                         "puesto" to puesto))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateTrabaja::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación de persona a compañía (trabaja):" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("fromId" to fromId))

            graphQLError.addExtraData("toId", toId)
            graphQLError.addExtraData("puesto", puesto)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:ALTA_TRABAJA_PERSONA_COMPAÑIA", value = graphQLError)
            return null
        }

        return res.body!!.data!!.createTrabaja
    }

    fun addDirects(idPersona: String, toIdArea: String, idCompania: String, nombreCompania: String): Dirige? {
        // In this bulk process we NOT check duplicates
        val addDirectGraphql = """
        mutation newDirige(${'$'}from: ID!, ${'$'}idCompania: Int!, ${'$'}nombreCompania: String!, ${'$'}to: ID!) {
            createDirige (from__id: ${'$'}from, idCompania:${'$'}idCompania, nombreCompania: ${'$'}nombreCompania, to__id:${'$'}to) {
                idCompania
                nombreCompania
            }
        }      
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addDirectGraphql,
                                            mutableMapOf("from" to idPersona,
                                                        "to" to toIdArea,
                                                        "idCompania" to idCompania,
                                                        "nombreCompania" to nombreCompania))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateDirige::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación de persona a area (dirige):" +  (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("person" to idPersona))

            graphQLError.addExtraData("toId", toIdArea)
            graphQLError.addExtraData("idCompania", idCompania)
            graphQLError.addExtraData("nombreCompania", nombreCompania)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:ALTA_AREA_DIRIGE", value = graphQLError)
            return null
        }

        return res.body!!.data!!.createDirige
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class PersonaCompania(val idPersona: Int,
                               val idCompania: Int,
                               val relacion: String,
                               val extraData: String)

    data class Trabaja(var to: CompaniaService.Compania? = null,
                       var puesto: String)

    data class GraphqlResponseCreateTrabaja(val data: Data? = null,
                                            val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createTrabaja: Trabaja)
    }

    data class Dirige(var to: AreaService.Area? = null,
                      var idCompania: String,
                      var nombreCompania: String)

    data class GraphqlResponseCreateDirige(val data: Data? = null,
                                           val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createDirige: Dirige)
    }
}