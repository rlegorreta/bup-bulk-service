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
 *  OrderServiceApplication.kt
 *
 *  Developed 2024 by LegoSoftSoluciones, S.C. www.legosoft.com.mx
 */
package com.ailegorreta.bupbulkservice

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.SerializationFeature
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder

/**
 * BUP bulk service main application
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2023
 *
 */
@SpringBootApplication(scanBasePackages = ["com.ailegorreta.resourceserver", "com.ailegorreta.bupbulkservice"])
class BupBulkServiceApplication {
	@Bean
	fun kotlinPropertyConfigurer(): PropertySourcesPlaceholderConfigurer {
		val propertyConfigurer = PropertySourcesPlaceholderConfigurer()

		propertyConfigurer.setPlaceholderPrefix("@{")
		propertyConfigurer.setPlaceholderSuffix("}")
		propertyConfigurer.setIgnoreUnresolvablePlaceholders(true)

		return propertyConfigurer
	}

	@Bean
	fun defaultPropertyConfigurer() = PropertySourcesPlaceholderConfigurer()

	@Bean
	fun mapperConfigurer() = Jackson2ObjectMapperBuilder().apply {
		serializationInclusion(JsonInclude.Include.NON_NULL)
		failOnUnknownProperties(true)
		featuresToDisable(*arrayOf(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS))
		indentOutput(true)

	}
}

fun main(args: Array<String>) {
	runApplication<BupBulkServiceApplication>(*args)
}
