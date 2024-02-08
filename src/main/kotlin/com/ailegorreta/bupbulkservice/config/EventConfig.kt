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
 *  EventConfig.kt
 *
 *  Developed 2023 by LegoSoftSoluciones, S.C. www.legosoft.com.mx
 */
package com.ailegorreta.bupbulkservice.config

import com.ailegorreta.bupbulkservice.service.*
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.function.Consumer

/**
 * Spring cloud stream kafka configuration.
 *
 * This class configure Kafka to listen to the events that come from the Spark ingestor.
 *
 * @author rlh
 * @project : bup-bulk-service
 * @date February 2024
 *
 */
@Configuration
class EventConfig {
    /**
     * This is the case when we receive the events from the Python Spark ingestor.
     */
    @Bean
    fun cPerson(personaService: PersonaService): Consumer<String> = Consumer {
            event: String -> personaService.processEvent(event)
    }

    @Bean
    fun cCompany(companiaService: CompaniaService): Consumer<String> = Consumer {
            event: String -> companiaService.processEvent(event)
    }

    @Bean
    fun cSubsidiary(subsidiariaService: SubsidiariaService): Consumer<String> = Consumer {
            event: String -> subsidiariaService.processEvent(event)
    }

    @Bean
    fun cArea(areaService: AreaService): Consumer<String> = Consumer {
            event: String -> areaService.processEvent(event)
    }

    @Bean
    fun cTelephone(telefonoService: TelefonoService): Consumer<String> = Consumer {
            event: String -> telefonoService.processEvent(event)
    }

    @Bean
    fun cAddress(direccionService: DireccionService): Consumer<String> = Consumer {
            event: String -> direccionService.processEvent(event)
    }

    @Bean
    fun cEmail(emailService: EmailService): Consumer<String> = Consumer {
            event: String -> emailService.processEvent(event)
    }

    @Bean
    fun cPersCos(personaCompaniaService: PersonaCompaniaService): Consumer<String> = Consumer {
            event: String -> personaCompaniaService.processEvent(event)
    }

    @Bean
    fun cCosCos(companiaCompaniaService: CompaniaCompaniaService): Consumer<String> = Consumer {
            event: String -> companiaCompaniaService.processEvent(event)
    }

    @Bean
    fun cPersPers(personaPersonaService: PersonaPersonaService): Consumer<String> = Consumer {
            event: String -> personaPersonaService.processEvent(event)
    }
}