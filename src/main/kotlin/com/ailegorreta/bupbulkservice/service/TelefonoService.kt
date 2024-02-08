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
 *  TelefonoService.kt
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
import org.springframework.security.oauth2.client.web.reactive.function.client.ServletOAuth2AuthorizedClientExchangeFilterFunction
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.util.UriComponentsBuilder
import reactor.core.publisher.Mono
import java.util.concurrent.CountDownLatch

/**
 * Service event consumer for relationship between Companies/Personas and Telephones.
 *
 * The for that it identifies between Company and Person is by IdPersona. First it checks that if 'IdPersona' in Personas
 * exist, if not, then it search for companies 'idPersona'.
 *
 * If the Telephone does not exist we insert it
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class TelefonoService(@Qualifier("client_credentials") val webClient: WebClient,
                      private val personaService: PersonaService,
                      private val companiaService: CompaniaService,
                      private val eventService: EventService,
                      private val serviceConfig: ServiceConfig,
                      private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * person/companies and telephones of the person/company
     */
    fun processEvent(event: String): String {

        try {
            val telephoneIdPersona = mapper.readValue(event, TelephoneIdPersona::class.java)

            logger.info("Received message from telephone:$telephoneIdPersona")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val telephone = addTelephoneIfNotExists(telephoneIdPersona)
                val persons = personaService.getPerson(telephoneIdPersona.idPersona)

                if (telephone != null) {
                    if (persons.size == 1)
                        addTelephonePerson(telephone.idNeo4j!!, persons.first().idNeo4j!!)
                    else {
                        val companies = companiaService.getCompany(idPersona = telephoneIdPersona.idPersona)

                        if (companies.size == 1)
                            addTelephoneCompany(telephone.idNeo4j!!, companies.first().idNeo4j!!)
                        else {
                            logger.error("Error al añadir leer el telefono ${telephoneIdPersona.numero} a la compañía ${telephoneIdPersona.idPersona}:")

                            eventService.sendEvent(
                                eventType = EventType.ERROR_EVENT,
                                headers = null, userName = "ingestor",
                                eventName = "ERROR:ASIGNACION_TELEFONO_COMPAÑIA", value = telephoneIdPersona
                            )
                            throw Exception("Error al añadir el teléfono a la compañía")
                        }
                    }
                } else {
                    logger.error("Error al añadir leer el telefono ${telephoneIdPersona.numero} al iDPersona ${telephoneIdPersona.idPersona}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:ASIGNACION_TELEFONO_ID_PERSONA", value = telephoneIdPersona)
                    throw Exception("Error al añadir el área a la compañía")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    private fun getTelephone(numero: String? = null): List<Telefono> {
        val variables = mutableMapOf("numero" to numero)
        val getTelehoneGraphql = """
            query getTelephone(${'$'}numero: String) {
              telefonoes(numero: ${'$'}numero) {
                _id
                numero
                ciudad
                tipo
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(getTelehoneGraphql, variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseTelefonos::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer el teléfono:" + res?.errors)
            return emptyList()
        }

        return res.data!!.telefonoes
    }

    fun addTelephoneIfNotExists(telephone: TelephoneIdPersona): Telefono? {
        // check that not exists
        val telefonos = getTelephone(telephone.numero)

        if (telefonos.isNotEmpty()) return telefonos.first()

        val addTelephoneGraphql = """ 
            mutation newTelephone(${'$'}numero: String!, ${'$'}ciudad: String!, ${'$'}tipo: String!) {
                createTelefono (numero: ${'$'}numero, ciudad: ${'$'}ciudad, tipo:${'$'}tipo) {
                    _id
                    numero
                    ciudad
                    tipo
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addTelephoneGraphql,
                            mutableMapOf("numero" to telephone.numero.filter { it.isDigit() },
                                         "ciudad" to telephone.ciudad,
                                         "tipo" to telephone.tipo))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateTelefono::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un teléfono:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_TELEFONO", value = EventGraphqlError(res.body?.errors, mutableMapOf("telephone" to telephone))
            )
            return null
        }

        return res.body!!.data!!.createTelefono
    }


    fun addTelephoneCompany(idTelephone: String, idCompany: String): Telefono {
        val addTelephoneCompany = """
            mutation(${'$'}id: ID!, ${'$'}compania:ID!) {
              addTelefonoCompania(_id:${'$'}id, compania:${'$'}compania) {
                _id
                numero
                ciudad
                tipo
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addTelephoneCompany,
                                                        mutableMapOf("id" to idTelephone,
                                                            "compania" to idCompany))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddTelefonoCompania::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación teléfono compañía:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idTelephone" to idTelephone))

            graphQLError.addExtraData("idCompany", idCompany)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:ALTA_TELEFONO_COMPAÑÏA", value = graphQLError)
            throw Exception("Error al añadir la relación teléfono compañía")
        }

        return res.body!!.data!!.addTelefonoCompania
    }

    fun addTelephonePerson(idTelephone: String, idPerson: String): Telefono {
        val addTelephonePerson = """
                mutation(${'$'}id: ID!, ${'$'}persona:ID!) {
                  addTelefonoPersona(_id:${'$'}id, persona:${'$'}persona) {
                    _id
                    numero
                    ciudad
                    tipo
                  }
                }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addTelephonePerson,
                                                    mutableMapOf("id" to idTelephone,
                                                                "persona" to idPerson))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddTelefonoPersona::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación teléfono persona:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idTelephone" to idTelephone))

            graphQLError.addExtraData("idPerson", idPerson)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:ALTA_TELEFONO_PERSONA", value = graphQLError)
            throw Exception("Error al añadir la relación teléfono persona")
        }

        return res.body!!.data!!.addTelefonoPersona
    }


    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Telefono(val _id: String?,
                        val numero: String,
                        val ciudad: String,
                        val tipo: String) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Telefono
            return numero == other.numero && _id == other._id
        }

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class TelephoneIdPersona(val idPersona: Int,
                                  val numero: String,
                                  val ciudad: String,
                                  val tipo: String)

    data class GraphqlResponseTelefonos(val data: Data? = null,
                                        val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val telefonoes: List<Telefono>)
        // ^ this is correct name since is the GraphQL generated schema
    }

    data class GraphqlResponseCreateTelefono(val data: Data? = null,
                                             val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createTelefono: Telefono)
    }

    /** Telefono -> Compania */
    data class GraphqlResponseAddTelefonoCompania(val data: Data? = null,
                                                  val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addTelefonoCompania: Telefono)
    }
    /** Telefono -> Persona 1:m auto-maintenance */
    data class GraphqlResponseAddTelefonoPersona(val data: Data? = null,
                                                 val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addTelefonoPersona: Telefono)
    }
}