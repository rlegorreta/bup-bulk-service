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
 *  EmailService.kt
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
 * Service event consumer for relationship between Personas and eMails.
 *
 * It insert a new mail server if not exist
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class EmailService(@Qualifier("client_credentials") val webClient: WebClient,
                      private val personaService: PersonaService,
                      private val eventService: EventService,
                      private val serviceConfig: ServiceConfig,
                      private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * person/companies and emails of the person/company
     */
    fun processEvent(event: String): String {

        try {
            val emailIdPersona = mapper.readValue(event, EmailIdPersona::class.java)

            logger.info("Received message from email:$emailIdPersona")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val emailServer = addEmailIfNotExists(emailIdPersona.email)
                val persons = personaService.getPerson(emailIdPersona.idPersona)

                if (emailServer != null && persons.size == 1) {
                    addEmailAsignado(from = persons.first().idNeo4j!!, to = emailServer!!.idNeo4j,
                                     email = emailIdPersona.email)
                } else {
                    logger.error("Error al añadir leer el email ${emailIdPersona.email} al iDPersona ${emailIdPersona.idPersona}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:ASIGNACION_EMAIL_ID_PERSONA", value = emailIdPersona)
                    throw Exception("Error al añadir el email a la persona")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }


    /**
     * This is not actually the email, it is the mail server
     */
    fun getEmail(uri: String): List<Email>? {
        val getEmailGraphql = """
            query getEmail(${'$'}uri: String) {
              emails(uri: ${'$'}uri) {
                _id
                uri
              }
            }            
        """.trimIndent()
        val variables = mutableMapOf("uri" to uri)
        val graphQLRequestBody = GraphqlRequestBody(getEmailGraphql,variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseEmails::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer los Emails servers:" + res?.errors)
            return null
        }

        return res.data!!.emails
    }

    fun addEmailIfNotExists(email: String): Email? {
        val emailUri = email.substringAfter('@')
        // check that not exists
        val emails = getEmail(uri = emailUri) ?: return null

        if (emails.isNotEmpty())
            return emails.first()

        val addEmailGraphql = """
             mutation newEmail(${'$'}uri: String!) {
                createEmail (uri: ${'$'}uri) {
                    _id
                    uri
                }
            }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addEmailGraphql,mutableMapOf("uri" to emailUri))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateEmail::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un email (server):" + (res?.body?.errors ?: ""))
            eventService.sendEvent(
                eventType = EventType.ERROR_EVENT, headers = res!!.headers,
                userName = "ingestor", eventName = "ERROR:ALTA_EMAIL_SERVER",
                value = EventGraphqlError(res.body?.errors, mutableMapOf("uri" to email))
            )
            return null
        }

        return res.body!!.data!!.createEmail
    }

    fun addEmailAsignado(from: String, to: String, email: String): EmailAsignado? {
        val addEmailAssignedGraphql = """
            mutation newEmailAsignado(${'$'}from: ID!, ${'$'}to: ID!, ${'$'}email: String!) {
                createEmailAsignado (from__id: ${'$'}from, to__id:${'$'}to, email: ${'$'}email) {
                    email
            	}
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addEmailAssignedGraphql,
                                                    mutableMapOf("from" to from,
                                                                 "to" to to,
                                                                 "email" to email))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateEmailAsignado::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación de persona a email (EmailAsignado):" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("from" to from))

            graphQLError.addExtraData("to", to)
            graphQLError.addExtraData("emailAssigned", email)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_EMAIL", value = graphQLError)
            return null
        }

        return res.body!!.data!!.createEmailAsignado
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Email(var _id: String,
                     var uri: String,
                     var emails: Collection<EmailAsignado>? ) {
        var idNeo4j = _id
    }

    data class EmailAsignado (var email: String)

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class EmailIdPersona(val idPersona: Int,
                              val email: String,)

    data class GraphqlResponseEmails(val data: Data? = null,
                                     val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val emails: List<Email>)
    }

    data class GraphqlResponseCreateEmail(val data: Data? = null,
                                          val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createEmail: Email)
    }

    data class GraphqlResponseCreateEmailAsignado(val data: Data? = null,
                                                  val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createEmailAsignado: EmailAsignado)
    }
}