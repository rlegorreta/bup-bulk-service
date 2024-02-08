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
 *  DireccionService.kt
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
 * Service event consumer for relationship between Companies/Personas and Address.
 *
 * The for that it identifies between Company and Person is by IdPersona. First it checks that if 'IdPersona' in Personas
 * exist, if not, then it search for companies 'idPersona'.
 *
 * The address procedure is the same as 'acme-ui', i.e., if zip code does not exist it is inserted and the same is
 * for municipio which is linked to estado.
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Service
class DireccionService(@Qualifier("client_credentials") val webClient: WebClient,
                      private val personaService: PersonaService,
                      private val companiaService: CompaniaService,
                      private val eventService: EventService,
                      private val serviceConfig: ServiceConfig,
                      private val mapper: ObjectMapper): HasLogger {
    var latch = CountDownLatch(1)
    val states = allStates()

    fun uri(): UriComponentsBuilder = UriComponentsBuilder.fromUriString(serviceConfig.getBupProvider())

    /**
     * This function receives an event from Kafka 'Python ingestor'' that we need to add a new relationship between
     * person/companies and address of the person/company
     */
    fun processEvent(event: String): String {

        try {
            val addressIdPersona = mapper.readValue(event, AddressIdPersona::class.java)

            logger.info("Received message from telephone:$addressIdPersona")
            if (serviceConfig.testing)
                latch.countDown()       // just for testing purpose
            else {
                val address = addAddress(addressIdPersona)

                if (address != null) {
                    val persons = personaService.getPerson(addressIdPersona.idPersona)

                    if (persons.size == 1)
                        addAddressPerson(idAddress = address.idNeo4j!!, idPerson = persons.first().idNeo4j!!)
                    else {
                        val companies = companiaService.getCompany(idPersona = addressIdPersona.idPersona)

                        if (companies.size == 1)
                            addAddressCompany(address.idNeo4j!!, companies.first().idNeo4j!!)
                        else {
                            logger.error("Error al añadir la dirección ${addressIdPersona.calle} a la compañía ${addressIdPersona.idPersona}:")

                            eventService.sendEvent( eventType = EventType.ERROR_EVENT,
                                headers = null, userName = "ingestor",
                                eventName = "ERROR:AÑADIR_DIRECCION_COMPAÑIA", value = addressIdPersona)
                            throw Exception("Error al añadir la dirección a la compañía")
                        }
                    }
                } else {
                    logger.error("Error al añadir la dirección ${addressIdPersona.calle} al idPersona ${addressIdPersona.idPersona}:")

                    eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                        headers = null, userName = "ingestor",
                        eventName = "ERROR:ASIGNACION_DIRECCION_ID_PERSONA", value = addressIdPersona
                    )
                    throw Exception("Error al añadir la dirección a la persona")
                }
            }
        } catch (e: JsonProcessingException) {
            throw SerializationException(e)
        }

        return event
    }

    fun addAddress(address: AddressIdPersona): Direccion? {
        val addAddressGraphql = """
            mutation newAddress(${'$'}calle: String!, ${'$'}ciudad: String, ${'$'}tipo: String!) {
                createDireccion (calle: ${'$'}calle, ciudad:${'$'}ciudad, tipo: ${'$'}tipo) {
                    _id
                    calle
                    ciudad
                    tipo
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAddressGraphql,
                                            mutableMapOf("calle" to address.calle,
                                                        "ciudad" to address.ciudad,
                                                        "tipo" to address.tipo))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateDireccion::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la dirección:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:AÑADIR_DIRECCION", value = EventGraphqlError(res.body?.errors, mutableMapOf("direccion" to address))
            )
            return null
        }
        // now its relationships to municipios and código.
        // note: we think they already where created before
        try {
            var zipcode = getZipcode(cp = address.codigo)

            if (zipcode == null || zipcode.size > 1)
                throw Exception("Error al leer el código postal ${address.codigo}")
            if (zipcode.isEmpty()) {
                val zc = addZipcode(cp = address.codigo, stateName = address.estado)
                if (zc == null)
                    throw Exception("Error al insertar el código postal ${address.codigo}")
                else
                    zipcode = listOf(zc)
            }
            addAddressZipcode(idAddress = res.body!!.data!!.createDireccion._id!!,
                              idZipcode = zipcode.first().idNeo4j!!)
            var colony = getColony(address.municipio)

            if (colony == null || colony.size > 1)
                throw Exception("Error al leer el municipio ${address.municipio}")
            else if (colony.isEmpty()) {
                val col = addColony(address.municipio, zipcode.first())

                if (col == null)
                    throw Exception("Error al insertar el municipio ${address.codigo}")
                else
                    colony = listOf(col)
            }
            addAddressColony(idAddress = res.body!!.data!!.createDireccion._id!!,
                             idColony = colony.first().idNeo4j!!)
        } catch (e: Exception) {
            logger.error("Error al añadir las relaciones de código postal y municipio:" + e.message)
            return null
        }

        return res.body!!.data!!.createDireccion
    }

    fun getZipcode(cp: Int? = null): List<Codigo>? {
        val variables = mutableMapOf("cp" to cp)
        val getZipcodeGraphql = """
            query getCodigo(${'$'}cp: Int) {
              codigoes(cp: ${'$'}cp) {
                _id
                cp
                estado {
                  _id
                  nombre
                  pais
                }
              }
            }            
        """.trimIndent()

        val graphQLRequestBody = GraphqlRequestBody(getZipcodeGraphql, variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseGetCodigos::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer los códigos postales:" + res?.errors)
            return null
        }

        return res.data!!.codigoes
    }

    fun addZipcode(cp: Int, stateName: String): Codigo? {
        val addZipcodeGraphql = """
            mutation newCodigo(${'$'}cp: Int!) {
                createCodigo (cp: ${'$'}cp) {
                    _id
                    cp
                }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addZipcodeGraphql, mutableMapOf("cp" to cp))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateCodigo::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir un código postal:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res?.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_CODIGO_POSTAL", value = EventGraphqlError(res?.body?.errors, mutableMapOf("zipcode" to cp, "state" to stateName))
            )
            return null
        }
        // now its relationships to states
        // note: we think that the states already where created before
        try {
            val state = states.find { it.nombre == stateName }
            if (state == null)
                throw Exception("Error estado $stateName no existe para relacionarlo al código $cp")
            addZipcodeState(idZipcode = res.body!!.data!!.createCodigo._id!!,
                            idState = state.idNeo4j!!)
        } catch (e: Exception) {
            logger.error("Error al añadir la relación de código postal a estado:" + e.message)
            return null
        }

        return res.body!!.data!!.createCodigo
    }

    final fun allStates(): List<Estado> {
        val allStatesGraphql = """
                 {
                  estadoes {
                    _id
                    nombre
                    pais
                  }
                }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(allStatesGraphql)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseEstados::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer el catálogo de estados:" + res?.errors)
            return emptyList()
        }

        return res.data!!.estadoes
    }

    private fun addZipcodeState(idZipcode: String, idState: String): Codigo {
        val addZipcodeStateGraphql = """
             mutation(${'$'}id: ID!, ${'$'}estado:ID!) {
              addCodigoEstado(_id:${'$'}id, estado:${'$'}estado) {
                _id
                cp
              }
            }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addZipcodeStateGraphql,
                                                    mutableMapOf("id" to idZipcode,
                                                                 "estado" to idState))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddCodigoEstado::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación código con estado:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idZipcode" to idZipcode))

            graphQLError.addExtraData("idState", idState)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res?.headers, userName = "ingestor",
                                    eventName = "ERROR:ASIGNAR_CODIGO_POSTAL_ESTADO", value = graphQLError)
            throw Exception("Error al añadir la relación código con estado")
        }

        return res.body!!.data!!.addCodigoEstado
    }

    private fun addAddressZipcode(idAddress: String, idZipcode: String): Direccion {
        val addAddressZipcodeGraphql = """
             mutation(${'$'}id: ID!, ${'$'}codigo:ID!) {
              addDireccionCodigo(_id:${'$'}id, codigo:${'$'}codigo) {
                _id
                calle
                ciudad
                tipo
              }
            }           
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAddressZipcodeGraphql,
                                                    mutableMapOf("id" to idAddress,
                                                                 "codigo" to idZipcode))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddDireccionCodigo::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación dirección con código:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idAddress" to idAddress))

            graphQLError.addExtraData("idZipcode", idZipcode)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:AÑADIR_DIRECCION_CODIGO", value = graphQLError)
            throw Exception("Error al añadir la relación dirección con código")
        }

        return res.body!!.data!!.addDireccionCodigo
    }

    fun getColony(name: String): List<Municipio>? {
        val getColonyGraphQl = """
            query getMunicipio(${'$'}nombre: String) {
              municipios(nombre: ${'$'}nombre) {
                _id
                nombre
                codigos {
                  _id
                  cp
                  estado {
                    nombre
                    pais
                  }
                }
              }
            }            
        """.trimIndent()
        val variables = mutableMapOf("nombre" to name)
        val graphQLRequestBody = GraphqlRequestBody(getColonyGraphQl, variables)
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .bodyToMono(GraphqlResponseMunicipios::class.java)
                            .block()

        if (res == null || res.errors != null) {
            logger.error("Error al leer catálogo de municipios:" + res?.errors)
            return null
        }

        return res.data!!.municipios
    }

    fun addColony(colony: String, zipcode: Codigo): Municipio? {
        val addColonyGraphql = """
            mutation newMunicipio(${'$'}nombre: String!) {
                createMunicipio (nombre: ${'$'}nombre) {
                    _id
                    nombre
            	}
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addColonyGraphql,
                                    mutableMapOf("nombre" to colony))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseCreateMunicipio::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir el municipio:" + (res?.body?.errors ?: ""))
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_MUNICIPIO", value = EventGraphqlError(res.body?.errors, mutableMapOf("colony" to colony))
            )
            return null
        }
        // now its relationships to zipcodes (at least on (i.e, the first one) must exist
        // note: we think they already where created before
        try {
            addColonyZipcode(idColony = res.body!!.data!!.createMunicipio._id!!,
                            idZipcode = zipcode.idNeo4j!!)
        } catch (e: Exception) {
            logger.error("Error al añadir la relación de código postal a estado:" + e.message)
            return null
        }

        return res.body!!.data!!.createMunicipio
    }

    fun addColonyZipcode(idColony: String, idZipcode: String): Municipio {
        val addColonyZipcodeGraphql = """
            mutation(${'$'}id: ID!, ${'$'}codigo:ID!) {
              addMunicipioCodigo(_id:${'$'}id, codigo:${'$'}codigo) {
                _id
                nombre
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addColonyZipcodeGraphql,
                                                        mutableMapOf("id" to idColony,
                                                                    "codigo" to idZipcode))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddMunicipioCodigo::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación municipio com código:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idColony" to idColony))

            graphQLError.addExtraData("idZipcode", idZipcode)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:ALTA_MUNICIPIO_CODIGO_POSTAL", value = graphQLError)
            throw Exception("Error al añadir la relación municipio con código")
        }

        return res.body!!.data!!.addMunicipioCodigo
    }


    private fun addAddressColony(idAddress: String, idColony: String): Direccion {
        val addAddressColonyGraphql = """
            mutation(${'$'}id: ID!, ${'$'}municipio:ID!) {
              addDireccionMunicipio(_id:${'$'}id, municipio:${'$'}municipio) {
                _id
                calle
                ciudad
                tipo
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAddressColonyGraphql,
                                                        mutableMapOf("id" to idAddress,
                                                                    "municipio" to idColony))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddDireccionMunicipio::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir la relación dirección con municipio:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idAddress" to idAddress))

            graphQLError.addExtraData("idColony", idColony)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                headers = res!!.headers, userName = "ingestor",
                eventName = "ERROR:AÑADIR_MUNICIPIO_DIRECCION", value = graphQLError)
            throw Exception("Error al añadir la relación dirección con municipio $idAddress $idColony")
        }

        return res.body!!.data!!.addDireccionMunicipio
    }

    fun addAddressPerson(idAddress: String, idPerson: String): Direccion {
        val addAddressPersonGraphql = """
            mutation(${'$'}id: ID!, ${'$'}persona:ID!) {
              addDireccionPersona(_id:${'$'}id, persona:${'$'}persona) {
                _id
                calle
                ciudad
                tipo
              }
            }
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAddressPersonGraphql,
                                                        mutableMapOf("id" to idAddress,
                                                                    "persona" to idPerson))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddDireccionPersona::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir le dirección a la persona:" +  (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idAddress" to idAddress))

            graphQLError.addExtraData("idPerson", idPerson)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:AÑADIR_DIRECCION_PERSONA", value = graphQLError)
            throw Exception("Error al añadir la dirección a la persona")
        }

        return res.body!!.data!!.addDireccionPersona
    }

    fun addAddressCompany(idAddress: String, idCompany: String): Direccion {
        val addAddressCompanyGraphql = """
            mutation(${'$'}id: ID!, ${'$'}compania:ID!) {
              addDireccionCompania(_id:${'$'}id, compania:${'$'}compania) {
                _id
                calle
                ciudad
                tipo
              }
            }            
        """.trimIndent()
        val graphQLRequestBody = GraphqlRequestBody(addAddressCompanyGraphql,
                                                    mutableMapOf("id" to idAddress,
                                                                "compania" to idCompany))
        val res = webClient.post()
                            .uri(uri().path("/bup/graphql").build().toUri())
                            .accept(MediaType.APPLICATION_JSON)
                            .body(Mono.just(graphQLRequestBody), GraphqlRequestBody::class.java)
                            .attributes(ServerOAuth2AuthorizedClientExchangeFilterFunction.clientRegistrationId(serviceConfig.clientId + "-client-credentials"))
                            .retrieve()
                            .toEntity(GraphqlResponseAddDireccionCompania::class.java)
                            .block()

        if ((res == null) || (res.body?.errors != null)) {
            logger.error("Error al añadir le dirección a la compañía:" + (res?.body?.errors ?: ""))
            val graphQLError = EventGraphqlError(res?.body?.errors, mutableMapOf("idAddress" to idAddress))

            graphQLError.addExtraData("idCompany", idCompany)
            eventService.sendEvent(eventType = EventType.ERROR_EVENT,
                                    headers = res!!.headers, userName = "ingestor",
                                    eventName = "ERROR:AÑADIR_DIRECCION_COMPANIA", value = graphQLError)
            throw Exception("Error al añadir la dirección a la compañía")
        }

        return res.body!!.data!!.addDireccionCompania
    }

    /**
     * ===== Data records
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    data class AddressIdPersona(val idPersona: Int,
                                val calle: String,
                                val ciudad: String,
                                val tipo: String,
                                val municipio: String,
                                val codigo: Int,
                                val estado: String) {
        override fun hashCode(): Int = idPersona.hashCode() + tipo.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as AddressIdPersona
            return idPersona == other.idPersona && tipo == other.tipo
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    data class Direccion(var _id: String?,
                         var calle: String,
                         var ciudad: String? = null,
                         var tipo: DireccionType,
                         var municipio: Municipio? = null,
                         var codigo: Codigo? = null) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Direccion
            return _id == other._id
        }
    }

    data class Municipio(var _id: String?,
                         var nombre: String,
                         var codigos: Collection<Codigo>? = null) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Municipio
            return _id == other._id
        }
    }

    data class Codigo(var _id: String?,
                      var cp: Int,
                      var estado: Estado? = null) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Codigo
            return cp == other.cp
        }
    }

    data class Estado(var _id: String?,
                      var nombre: String,
                      var pais: String) {
        var idNeo4j = _id

        override fun hashCode(): Int = _id.hashCode()

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false
            other as Estado
            return nombre == other.nombre && pais == other.pais
        }
    }

    enum class DireccionType {
        OFICIAL, CASA, TEMPORAL, FACTURAR
    }

    data class GraphqlResponseCreateDireccion(val data: Data? = null,
                                              val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createDireccion: Direccion)
    }

    data class GraphqlResponseGetCodigos(val data: Data? = null,
                                         val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val codigoes: List<Codigo>)
        // ^ this is correct name since is the GraphQL generated schema
    }

    data class GraphqlResponseCreateCodigo(val data: Data? = null,
                                           val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createCodigo: Codigo)
    }

    data class GraphqlResponseEstados(val data: Data? = null,
                                      val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val estadoes: List<Estado>)
        // ^ this is correct name since is the GraphQL generated schema
    }

    /** Codigo -> Estado */
    data class GraphqlResponseAddCodigoEstado(val data: Data? = null,
                                              val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addCodigoEstado: Codigo)
    }
    /** Direccion -> Codigo 1: 1 */
    data class GraphqlResponseAddDireccionCodigo(val data: Data? = null,
                                                 val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addDireccionCodigo: Direccion)
    }

    data class GraphqlResponseMunicipios(val data: Data? = null,
                                         val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val municipios: List<Municipio>)
    }

    data class GraphqlResponseCreateMunicipio(val data: Data? = null,
                                              val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val createMunicipio: Municipio)
    }

    /** Municipio -> Codigo could be several 1:m  */
    data class GraphqlResponseAddMunicipioCodigo(val data: Data? = null,
                                                 val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addMunicipioCodigo: Municipio)
    }

    /** Direccion -> Municipio  1: 1 */
    data class GraphqlResponseAddDireccionMunicipio(val data: Data? = null,
                                                    val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addDireccionMunicipio: Direccion)
    }

    /** Direccion -> Persona 1:m */
    data class GraphqlResponseAddDireccionPersona(val data: Data? = null,
                                                  val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addDireccionPersona: Direccion)
    }

    /** Direccion -> Compania 1:m */
    data class GraphqlResponseAddDireccionCompania(val data: Data? = null,
                                                   val errors: Collection<Map<String, Any>>? = null) {
        data class Data(val addDireccionCompania: Direccion)
    }
}