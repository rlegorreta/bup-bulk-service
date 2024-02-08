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
 *  PersonaService.kt
 *
 *  Developed 2024 by LegoSoftSoluciones, S.C. www.legosoft.com.mx
 */
package com.ailegorreta.bupbulkservice.service

import com.ailegorreta.bupbulkservice.config.ServiceConfig
import com.ailegorreta.bupbulkservice.data.GraphqlRequestBody
import com.ailegorreta.commons.event.EventGraphqlError
import com.ailegorreta.commons.event.EventType
import com.ailegorreta.commons.utils.HasLogger
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
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CountDownLatch

/**
 * Service event consumer for Personas
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class PersonaService(@Qualifier("client_credentials") val webClient: WebClient,
                     private val rfcService: RfcService,
                     private val eventService: EventService,
                     private val serviceConfig: ServiceConfig,
                     private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new Persona
     */
    fun processEvent(event: String): String {

        try {
            val persona = mapper.readValue(event, Persona::class.java)

            logger.info("Received message from persona:$persona")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else
                addPerson(persona)
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addPerson(person: Persona): Persona? {
        val persons =getPerson(person.idPersona)  // maybe it can exist because the message is received more than once
        var newPerson: Persona

        if (persons.isNotEmpty()) {
            logger.warn("La persona ${person.idPersona} YA existe en la base de datos no se inserta el registro")
            newPerson = persons.first()
        } else {
            val addPersonGraphql = """
                        mutation newPersonas(${'$'}nombre: String!,
                                             ${'$'}apellidoPaterno: String!,
                                             ${'$'}apellidoMaterno: String!,
                                             ${'$'}fechaNacimiento: LocalDate,
                                             ${'$'}genero: String!,
                                             ${'$'}estadoCivil: String!,
                                             ${'$'}curp: String!,
                                             ${'$'}usuarioModificacion: String
                                             ${'$'}fechaModificacion: LocalDateTime,
                                             ${'$'}activo: Boolean!
                                             ${'$'}idPersona: Int) {
                            createPersona (nombre: ${'$'}nombre,
                                           apellidoPaterno: ${'$'}apellidoPaterno,
                                           apellidoMaterno: ${'$'}apellidoMaterno,
                                           fechaNacimiento: ${'$'}fechaNacimiento,
                                           genero: ${'$'}genero,
                                           estadoCivil: ${'$'}estadoCivil,
                                           curp: ${'$'}curp,
                                           usuarioModificacion:${'$'}usuarioModificacion,
                                           fechaModificacion:${'$'}fechaModificacion,
                                           activo: ${'$'}activo,
                                           idPersona: ${'$'}idPersona) {
                                _id
                                nombre
                                apellidoPaterno
                                apellidoMaterno
                                fechaNacimiento
                                genero
                                estadoCivil
                                curp
                                usuarioModificacion
                                fechaModificacion
                                activo
                                idPersona
                            }
                        }            
            """.trimIndent()

            val graphQLRequestBody = GraphqlRequestBody(addPersonGraphql,
                                                            mutableMapOf(
                                                                "nombre" to person.nombre,
                                                                "apellidoPaterno" to person.apellidoPaterno,
                                                                "apellidoMaterno" to person.apellidoMaterno,
                                                                "fechaNacimiento" to person.fechaNacimiento,
                                                                "genero" to person.genero,
                                                                "estadoCivil" to person.estadoCivil,
                                                                "curp" to person.curp,
                                                                "usuarioModificacion" to person.usuarioModificacion,
                                                                "fechaModificacion" to person.fechaModificacion.format(DateTimeFormatter.ISO_DATE_TIME),
                                                                "activo" to person.activo,
                                                                "idPersona" to person.idPersona
                                                            )
                                                        )
            val res = webClient.post()
                                .uri(uri().path("/bup/graphql").build().toUri())
                                .accept(MediaType.APPLICATION_JSON)
                                .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                                .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                                .retrieve()
                                .toEntity(GraphqlResponseCreatePersona::class.java)
                                .block()

            if ((res == null) || (res.body?.errors != null)) {
                logger.error("Error al añadir la persona:" + (res?.body?.errors ?: ""))
                eventService.sendEvent(
                    eventType = EventType.ERROR_EVENT,
                    headers = res!!.headers,
                    userName = "ingestor",
                    eventName = "ERROR:ALTA_PERSONA",
                    value = EventGraphqlError(res.body?.errors, mutableMapOf("person" to person))
                )
                return null
            }
            newPerson = res.body!!.data!!.createPersona

            eventService.sendEvent(
                headers = res.headers, userName = "bup-bulk-service",
                eventName = "ALTA_PERSONA", value = newPerson
            )
            // ^ this is to synchronize the IAM database
        }
        if (person.rfc != null) {
            val newRfc = rfcService.addRfcIfNotExists(person.rfc)

            if (newRfc != null)
                addPersonRfc(newPerson.idNeo4j!!, newRfc.idNeo4j!!)
        }

        return newPerson
    }

    fun getPerson(idPersona: Int): List<Persona> {
        val variables = mutableMapOf("idPersona" to idPersona)
        val getPersonGraphql = """
            query getPerson(${'$'}idPersona:Int) {
                personae(idPersona:${'$'}idPersona) {
                        _id
                        nombre
                        apellidoPaterno
                        apellidoMaterno
                        fechaNacimiento
                        genero
                        estadoCivil
                        curp
                        usuarioModificacion
                        fechaModificacion
                        activo
                        idPersona
                  }
            }
            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(getPersonGraphql, variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponsePersonas::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer la persona:" + res?.errors)
            return emptyList()
        }

        return res.data!!.personae
    }

    /**
     * Company rfc relationship maintenance
     */
    private fun addPersonRfc(idPerson: String, idRfc: String): Persona {
        val addPersonRfcGraphql = """
            mutation(${'$'}id: ID!, ${'$'}rfc:ID!) {
                addPersonaRfc(_id:${'$'}id, rfc:${'$'}rfc) {
                    _id
                    nombre
                    apellidoPaterno
                    apellidoMaterno
                    fechaNacimiento
                    genero
                    estadoCivil
                    curp
                    usuarioModificacion
                    fechaModificacion
                    activo
                    idPersona
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addPersonRfcGraphql,
                                                mutableMapOf("id" to idPerson,
                                                             "rfc" to idRfc))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddPersonaRfc::class.java)
                            .block()

        if ((res == null) || (res.body!!.errors != null)) {
            logger.error("Error al añadir la relación persona rfc:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idPerson" to idPerson))

            graphQLError.addExtraData("idRfc", idRfc)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                    headers = res!!.headers, userName = "ingestor",
                    eventName = "ERROR:ASIGNACION_RFC_PERSONA", value = graphQLError)
            throw Exception("Error al añadir la relación persona rfc")
        }

        return res.body!!.data!!.addPersonaRfc
    }

    /**
     * === Data records
     */
    data class Persona(var _id: String?,
                       val nombre: String,
                       val apellidoPaterno: String,
                       val apellidoMaterno: String,
                       val fechaNacimiento: LocalDate,
                       val genero: String,
                       val estadoCivil: String,
                       val curp: String,
                       val rfc: String? = null,
                       val usuarioModificacion: String = "ingestor",
                       val fechaModificacion: LocalDateTime = LocalDateTime.now(),
                       val idPersona: Int,
                       val activo: Boolean = true) {

        var idNeo4j = _id       // this is because the mapping done from the endpoint with '_id' it ignores de underscore
                                // and make it wrong
    }

    data class GraphqlResponseCreatePersona(val data: Data? = null,
                                            val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createPersona: Persona)
    }

    data class GraphqlResponsePersonas(val data: Data? = null,
                                       val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val personae: List<Persona>)
    }

    /** Persona -> Rfc */
    data class GraphqlResponseAddPersonaRfc(val data: Data? = null,
                                            val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addPersonaRfc: Persona)
    }

}