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
 *  AreaService.kt
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
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.client.web.reactive.function.client.ServerOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import java.util.concurrent.CountDownLatch

/**
 * Service event consumer for relationship between Companies and Areas
 *
 * If the Area does not exist we insert it
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class AreaService(@Qualifier("client_credentials") val webClient: WebClient,
                         private val companiaService: CompaniaService,
                         private val eventService: EventService,
                         private val serviceConfig: ServiceConfig,
                         private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * companies and areas of the company
     */
    fun processEvent(event: String): String {

        try {
            val areaCompany = mapper.readValue(event, AreaCompany::class.java)

            logger.info("Received message from company:$areaCompany")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val area = addAreaIfNotExists(areaCompany.area)
                val companies = companiaService.getCompany(name = areaCompany.compania)

                if (area != null && companies.size == 1)
                    addAreaCompany(area.idNeo4j!!, companies.first().idNeo4j!!)
                else {
                    logger.error("Error al añadir leer el área ${areaCompany.area} a la compañía ${areaCompany.compania}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:ASIGNACION_AREA_COMPAÑIA", value = areaCompany)
                    throw Exception("Error al añadir el área a la compañía")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun getArea(nombre: String): List<Area> {
        val variables = mutableMapOf("nombre" to nombre)
        val getAreaGraphql = """
            query getArea(${'$'}nombre: String) {
              areas(nombre: ${'$'}nombre) {
                _id
                nombre
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(getAreaGraphql,variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseAreas::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer el área $nombre:" + res?.errors)
            return emptyList()
        }

        return res.data!!.areas
    }

    fun addAreaIfNotExists(nombre: String): Area? {
        // check that not exists
        val areas = getArea(nombre = nombre)

        if (areas.isNotEmpty()) return areas.first()

        val addAreaGraphql = """
            mutation newArea(${'$'}nombre: String!) {
                createArea (nombre: ${'$'}nombre) {
                    _id
                    nombre
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAreaGraphql,
                                            mutableMapOf("nombre" to nombre))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateArea::class.java)
                            .block()

        if (res == null || res.body!!.errors != null) {
            logger.error("Error al añadir una área:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = SecurityContextHolder.getContext().authentication!!.name,
                eventName = "ERROR:ALTA_AREA_COMPAÑIA", value = EventGraphqlError(res.body?.errors, mutableMapOf("area" to nombre))
            )
            return null
        }

        return res.body!!.data!!.createArea
    }

    fun addAreaCompany(idArea: String, idCompany: String): Area {
        val addAreaCompanyGraphql = """
            mutation(${'$'}id: ID!, ${'$'}compania:ID!) {
              addAreaCompania(_id:${'$'}id, compania:${'$'}compania) {
                _id
                nombre
              }
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAreaCompanyGraphql,
                                            mutableMapOf("id" to idArea,
                                                         "compania" to idCompany))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddAreaCompania::class.java)
                            .block()

        if ((res == null) || (res.body!!.errors != null)) {
            logger.error("Error al añadir la relación área compañía:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idArea" to idArea))

            graphQLError.addExtraData("idCompany", idCompany)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = SecurityContextHolder.getContext().authentication!!.name,
                eventName = "ERROR:ASIGNACION_AREA_COMPAÑIA", value = graphQLError)

            throw Exception("Error al añadir la relación área compañía")
        }

        return res.body!!.data!!.addAreaCompania
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Area(val _id: String?,
                    val nombre: String) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Area
            return nombre == other.nombre
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class AreaCompany(val compania: String, val area: String)

    data class GraphqlResponseAreas(val data: Data? = null,
                                    val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val areas: List<Area>)
    }

    data class GraphqlResponseCreateArea(val data: Data? = null,
                                         val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createArea: Area)
    }

    /** Area -> Compania */
    data class GraphqlResponseAddAreaCompania(val data: Data? = null,
                                              val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addAreaCompania: Area)
    }
}