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
 *  CompaniaService.kt
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
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch

/**
 * Service event consumer for Companies
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class CompaniaService(@Qualifier("client_credentials") val webClient: WebClient,
                      private val rfcService: RfcService,
                      private val eventService: EventService,
                      private val serviceConfig: ServiceConfig,
                      private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())
    val sectors: MutableMap<String, String> = mutableMapOf()

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new Company
     */
    fun processEvent(event: String): String {

        try {
            val company = mapper.readValue(event, Compania::class.java)

            logger.info("Received message from company:$company")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else
                addCompany(company)
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addCompany(company: Compania): Compania? {
        val companies = getCompany(company.idPersona) // maybe it can exist because the message is received more than once
        val newCompany: Compania

        if (companies.isNotEmpty()) {
            logger.warn("La compania ${company.idPersona} YA existe en la base de datos no se inserta el registro")
            newCompany = companies.first()
        } else {
            val addCompanyGraphql = """
                mutation newCompania(${'$'}nombre: String!,
                                     ${'$'}usuarioModificacion: String!,
                                     ${'$'}fechaModificacion: LocalDateTime!,
                                     ${'$'}padre: Boolean!,
                                     ${'$'}activo: Boolean!,
                                     ${'$'}idPersona: Int!) {
                    createCompania (nombre: ${'$'}nombre,
                                    usuarioModificacion:${'$'}usuarioModificacion,
                                    fechaModificacion:${'$'}fechaModificacion,
                                    padre:${'$'}padre,
                                    activo: ${'$'}activo,
                                    idPersona: ${'$'}idPersona) {
                        _id
                        nombre
                        usuarioModificacion
                        fechaModificacion
                        padre
                        activo
                        idPersona                    
                    }
                }            
            """.trimIndent()
            val graphQLRequestBody = GraphqlRequestBody(addCompanyGraphql,
                                        mutableMapOf(
                                            "nombre" to company.nombre,
                                            "usuarioModificacion" to company.usuarioModificacion,
                                            "fechaModificacion" to company.fechaModificacion.format(DateTimeFormatter.ISO_DATE_TIME),
                                            "padre" to company.padre,
                                            "activo" to company.activo,
                                            "idPersona" to company.idPersona
                                        )
                                    )
            val res = webClient.post()
                                .uri(uri().path("/bup/graphql").build().toUri())
                                .accept(MediaType.APPLICATION_JSON)
                                .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                                .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                                .retrieve()
                                .toEntity(GraphqlResponseCreateCompania::class.java)
                                .block()

            if ((res == null) || (res.body?.errors != null)) {
                logger.error("Error al añadir a la compañía:" + (res?.body?.errors ?: ""))
                eventService.sendEvent(
                    eventType = EventType.ERROR_EVENT,
                    headers = res?.headers,
                    userName = company.usuarioModificacion,
                    eventName = "ERROR:ALTA_COMPANIA",
                    value = EventGraphqlError(res?.body?.errors, mutableMapOf("company" to company))
                )
                return null
            }
            newCompany = res.body!!.data!!.createCompania

            eventService.sendEvent(
                headers = res.headers, userName = company.usuarioModificacion,
                eventName = "ALTA_COMPANIA", value = res.body?.data!!.createCompania)
            // Inform Kafka event to keep in sync with the IAM database
        }
        if (company.negocio.isNotEmpty())
            addCompanySector(newCompany.idNeo4j!!, company.negocio)
        if (company.rfc != null) {
            val newRfc = rfcService.addRfcIfNotExists(company.rfc)

            if (newRfc != null)
                addCompanyRfc(newCompany.idNeo4j!!, newRfc.idNeo4j!!)
        }

        return newCompany
    }

    /**
     * Company sector relationship. Keep all sectors in memory. We suppose it is a small List
     */
    fun allSectors(): List<Sector> {
        val allSectorsGraphql = """
            {
              sectors {
                _id
                nombre
              }
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(allSectorsGraphql)
        val res = webClient.post()
                                .uri(uri().path("/bup/graphql").build().toUri())
                                .accept(MediaType.APPLICATION_JSON)
                                .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                                .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                                .retrieve()
                                .bodyToMono(GraphqlResponseSectors::class.java)
                                .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer sectores:" + res?.errors)
            return emptyList()
        }

        return res.data!!.sectors
    }

    private fun addCompanySector(idCompany: String, sectorName: String): Compania {
        if (sectors.isEmpty())
            allSectors().forEach { sectors[it.nombre] = it._id }  // initialize Sector catalog
        val addCompanySectorGraphQL = """
             mutation(${'$'}id: ID!, ${'$'}sector:ID!) {
              addCompaniaSector(_id:${'$'}id, sector:${'$'}sector) {
                _id
                nombre
              }
            }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addCompanySectorGraphQL,
                                                    mutableMapOf("id" to idCompany,
                                                                 "sector" to sectors[sectorName]))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddCompaniaSector::class.java)
                            .block()

        if ((res == null) || (res.body!!.errors != null)) {
            logger.error("Error al añadir la relación compañía sector:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idCompany" to idCompany))

            graphQLError.addExtraData("sector", sectorName)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:ASIGNACION_SECTOR_COMPAÑIA", value = graphQLError)
            throw Exception("Error al añadir la relación compañía sector")
        }

        return res.body!!.data!!.addCompaniaSector
    }

    /**
     * Get Company by name or idPersone to retrieve de neo4h ID
     */
    fun getCompany(idPersona: Int? = null, name: String? = null): List<Compania> {
        val variables = if (idPersona == null) mutableMapOf("nombre" to name)
                        else mutableMapOf("idPersona" to idPersona)
        val getCompanyGraphql = if (idPersona == null) """
                query getCompany(${'$'}nombre: String) {
                    companias(nombre: ${'$'}nombre) {
                        _id
                        nombre
                        usuarioModificacion
                        fechaModificacion
                        padre
                        activo
                        idPersona
                    }
                }                
            """.trimIndent()
        else """
                 query getCompany(${'$'}idPersona: Int) {
                    companias(idPersona: ${'$'}idPersona) {
                        _id
                        nombre
                        usuarioModificacion
                        fechaModificacion
                        padre
                        activo
                        idPersona
                    }
                }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(getCompanyGraphql,variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseCompanias::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer la compañía:" + res?.errors)
            return emptyList()
        }

        return res.data!!.companias
    }

    /**
     * Company rfc relationship maintenance
     */
    private fun addCompanyRfc(idCompany: String, idRfc: String): Compania {
        val addCompanyRfcGraphql = """
            mutation(${'$'}id: ID!, ${'$'}rfc:ID!) {
                addCompaniaRfc(_id:${'$'}id, rfc:${'$'}rfc) {
                    _id
                    nombre
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addCompanyRfcGraphql,
                                                    mutableMapOf("id" to idCompany,
                                                                "rfc" to idRfc))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddCompaniaRfc::class.java)
                            .block()

        if ((res == null) || (res.body!!.errors != null)) {
            logger.error("Error al añadir la relación compañía rfc:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idCompany" to idCompany))

            graphQLError.addExtraData("idRfc", idRfc)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                            headers = res!!.headers, userName = "ingestor",
                            eventName = "ERROR:ASIGNACION_RFC_COMPAÑIA", value = graphQLError)
            throw Exception("Error al añadir la relación compañía rfc")
        }

        return res.body!!.data!!.addCompaniaRfc
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Compania(val _id: String? = null,
                        val nombre: String,
                        val negocio: String = "",
                        val rfc: String? = null,
                        val usuarioModificacion: String = "ingestor",
                        val fechaModificacion: LocalDateTime = LocalDateTime.now(),
                        val idPersona: Int,
                        val activo: Boolean = true,
                        val padre: Boolean = true) {

        var idNeo4j = _id       // this is because the mapping done from the endpoint with '_id' it ignores de underscore
                                // and make it wrong
    }

    data class GraphqlResponseCompanias(val data: Data? = null,
                                        val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val companias: List<Compania>)
    }
    data class GraphqlResponseCreateCompania(val data: Data? = null,
                                             val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createCompania: Compania)
    }

    /** Compania -> Sector */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Sector(var _id: String, var nombre: String)

    data class GraphqlResponseSectors(val data: Data? = null,
                                      val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val sectors: List<Sector>)
    }

    data class GraphqlResponseAddCompaniaSector(val data: Data? = null,
                                                val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addCompaniaSector: Compania)
    }

    /** Compania -> Rfc */
    data class GraphqlResponseAddCompaniaRfc(val data: Data? = null,
                                             val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addCompaniaRfc: Compania)
    }
}