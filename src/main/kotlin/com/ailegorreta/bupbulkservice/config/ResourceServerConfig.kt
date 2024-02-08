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
 *  ResourceServerConfig.kt
 *
 *  Developed 2024 by LegoSoftSoluciones, S.C. www.legosoft.com.mx
 */
package com.ailegorreta.bupbulkservice.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.config.Customizer
import org.springframework.security.config.annotation.web.reactive.EnableWebFluxSecurity
import org.springframework.security.config.web.server.ServerHttpSecurity
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter
import org.springframework.security.web.server.SecurityWebFilterChain
import org.springframework.security.web.server.csrf.CsrfToken
import org.springframework.security.web.server.savedrequest.NoOpServerRequestCache
import org.springframework.web.server.WebFilter
import reactor.core.publisher.Mono

/**
 * Resource server configuration. Nobody calls this microservice, via REST
 *
 * One scope is are defined for this resource:
 *  -acme.facultad
 *
 * @author rlh
 * @project : bup-cluster-service
 * @date February 2024
 *
 */
@EnableWebFluxSecurity
@Configuration(proxyBeanMethods = false)
class ResourceServerConfig {

    /**
     *  -- This code is we want for develop purpose to use all REST calls without a token --
     *  -- For example: if we want to run the REST from swagger and test the microservice
     * http.authorizeHttpRequests{ auth ->  auth
     *     .requestMatchers("/ **").permitAll()
     *     .anyRequest().authenticated()
     *
     * note: erse white space between '/ **' ) just for comment
     *
     **/

    // @formatter:off
    @Bean
    @Throws(Exception::class)
    fun reactiveSecurityFilterChain( http:ServerHttpSecurity): SecurityWebFilterChain {
        http.authorizeExchange{ oauth2 ->  oauth2
            .pathMatchers("/actuator/**").permitAll()
            .anyExchange().authenticated()
        }
            .oauth2ResourceServer{ server -> server.jwt { Customizer.withDefaults<Any>()}}
            .requestCache{ requestCacheSpec ->
                requestCacheSpec.requestCache(NoOpServerRequestCache.getInstance())
            }
            .csrf(ServerHttpSecurity.CsrfSpec::disable)

        return http.build()
    }
    // @formatter:on

    /**
     * Extracting roles from the Access Token
     */
    @Bean
    fun jwtAuthenticationConverter(): JwtAuthenticationConverter {
        val jwtGrantedAuthoritiesConverter = JwtGrantedAuthoritiesConverter()

        jwtGrantedAuthoritiesConverter.setAuthorityPrefix("ROLE_");
        // ^ Applies the “ROLE_” prefix to each user role
        jwtGrantedAuthoritiesConverter.setAuthoritiesClaimName("roles")
        // ^ Extracts the list of roles from the roles claim

        var jwtAuthenticationConverter = JwtAuthenticationConverter()
        // ^ Defines a converter to map claims to GrantedAuthority objects

        jwtAuthenticationConverter .setJwtGrantedAuthoritiesConverter(jwtGrantedAuthoritiesConverter)

        return jwtAuthenticationConverter
    }

    /**
     * At the moment, CookieServerCsrfTokenRepository does not ensure a subscription to CsrfToken, so we must
     * explicitly provide a workaround in a Web-Filter bean. This problem should be solved in future versions of
     * Spring Security (see issue 5766 on GitHub: https://mng.bz/XW89)
     */
    @Bean
    fun csrfWebFilter(): WebFilter {
        // A filter with the only purpose of subscribing to the CsrfToken reactive stream and ensuring its value is extracted correctly
        return WebFilter { exchange, chain ->
            exchange.response.beforeCommit {
                Mono.defer {
                    val csrfToken: Mono<CsrfToken>? = exchange.getAttribute(CsrfToken::class.java.name)
                    csrfToken?.then() ?: Mono.empty()
                }
            }
            chain.filter(exchange)
        }
    }
}