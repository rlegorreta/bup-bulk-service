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
 *  SubsidiariaService.kt
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
 * Service event consumer for relationship between Companies and Subsidiaries
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class SubsidiariaService(@Qualifier("client_credentials") val webClient: WebClient,
                         private val companiaService: CompaniaService,
                         private val eventService: EventService,
                         private val serviceConfig: ServiceConfig,
                         private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * companies and subsidiary company
     */
    fun processEvent(event: String): String {

        try {
            val subsidiaryCompany = mapper.readValue(event, SubsidiariaCompania::class.java)

            logger.info("Received message from company:$subsidiaryCompany")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val subsidiaries = companiaService.getCompany(name = subsidiaryCompany.subsidiaria)
                val companies = companiaService.getCompany(name = subsidiaryCompany.compania)

                if (subsidiaries.size == 1 && companies.size == 1)
                    addCompanySubsidiary(companies.first().idNeo4j!!, subsidiaries.first().idNeo4j!!)
                else {
                    logger.error("Error al añadir leer la subsidiaria y/o compañía :")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                            headers = null, userName = "ingestor",
                            eventName = "ERROR:ASIGNACION_SUBSIDIARIA_COMPAÑIA", value = subsidiaries)
                    throw Exception("Error al añadir la subsidiaria a la compañía")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addCompanySubsidiary(idCompany: String, idSubsidiary: String): CompaniaService.Compania {
        val addCompanySubsidiaryGraphql = """
            mutation(${'$'}id: ID!, ${'$'}subsidiaria:ID!) {
              addCompaniaSubsidiaria (_id:${'$'}id, subsidiaria:${'$'}subsidiaria) {
                _id
                nombre
              }
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addCompanySubsidiaryGraphql,
            mutableMapOf("id" to idCompany,
                         "subsidiaria" to idSubsidiary))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddCompaniaSubsidiaria::class.java)
                            .block()

        if (res == null || res.body?.errors != null) {
            logger.error("Error al añadir la subsidiaria a la compañía :" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idCompany" to idCompany))

            graphQLError.addExtraData("idSubsidiary", idSubsidiary)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ASIGNACION_SUBSIDIARIA_COMPAÑIA", value = graphQLError)
            throw Exception("Error al añadir la subsidiaria a la compañía")
        }

        return res.body!!.data!!.addCompaniaSubsidiaria
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class SubsidiariaCompania(val subsidiaria: String,
                                   val compania: String)

    /** Compania -> Compania (subsidiaria) */
    data class GraphqlResponseAddCompaniaSubsidiaria(val data: Data? = null,
                                                     val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addCompaniaSubsidiaria: CompaniaService.Compania)
    }
}