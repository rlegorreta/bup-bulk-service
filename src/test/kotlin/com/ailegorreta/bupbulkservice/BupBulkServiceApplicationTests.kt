package com.ailegorreta.bupbulkservice

import com.ailegorreta.bupbulkservice.service.CompaniaService
import com.ailegorreta.bupbulkservice.service.PersonaService
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.cloud.stream.function.StreamBridge
import org.springframework.context.annotation.Bean
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.security.oauth2.jwt.ReactiveJwtDecoder
import org.springframework.test.context.ActiveProfiles

/**
 * For a good test slices for testing @SpringBootTest, see:
 * https://reflectoring.io/spring-boot-test/
 * https://www.diffblue.com/blog/java/software%20development/testing/spring-boot-test-slices-overview-and-usage/
 *
 * This class test all context with @SpringBootTest annotation and checks that everything is loaded correctly.
 * Also creates the classes needed for all slices in @TestConfiguration annotation
 *
 * Testcontainers:
 *
 * Use for test containers Kafka following the next's ticks:
 *
 * - As little overhead as possible:
 * - Containers are started only once for all tests
 * - Containers are started in parallel
 * - No requirements for test inheritance
 * - Declarative usage.
 *
 * see article: https://maciejwalkowiak.com/blog/testcontainers-spring-boot-setup/
 *
 * Also for a problem with bootstrapServerProperty
 * see: https://blog.mimacom.com/embeddedkafka-kafka-auto-configure-springboottest-bootstrapserversproperty/
 *
 * @project bup-bulk-service-service
 * @author: rlh
 * @date: February 2024
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnableTestContainers
/* ^ This is a custom annotation to load the containers */
@ExtendWith(MockitoExtension::class)
@EmbeddedKafka(bootstrapServersProperty = "spring.kafka.bootstrap-servers")
/* ^ this is because: https://blog.mimacom.com/embeddedkafka-kafka-auto-configure-springboottest-bootstrapserversproperty/ */
@ActiveProfiles("integration-tests")
class BupBulkServiceApplicationTests {
	/* StreamBridge instance is used by EventService but in @Test mode it is not instanciated, so we need to mock it:
        see: https://stackoverflow.com/questions/67276613/streambridge-final-cannot-be-mocked
        StreamBridge is a final class, With Mockito2 we can mock the final class, but by default this feature is disabled
        and that need to enable with below steps:

        1. Create a directory ‘mockito-extensions’ in src/test/resources/ folder.
        2. Create a file ‘org.mockito.plugins.MockMaker’ in ‘src/test/resources/mockito-extensions/’ directory.
        3. Write the content 'mock-maker-inline' in org.mockito.plugins.MockMaker file.

        At test class level use ‘@ExtendWith(MockitoExtension.class)’
        Then StreamBridge will be mocked successfully.

        note: Instead of mocking the final class (which is possible with the latest versions of mockito using the
        mock-maker-inline extension), you can wrap StreamBridge into your class and use it in your business logic.
        This way, you can mock and test it any way you need.

        This is a common practice for writing unit tests for code where some dependencies are final or static classes
    */
	@MockBean
	private val streamBridge: StreamBridge? = null
	@MockBean
	private var reactiveJwtDecoder: ReactiveJwtDecoder? = null			// Mocked the security JWT

	@Autowired
	private val personaService: PersonaService? = null
	@Autowired
	private val companiaService: CompaniaService? = null

	@Test
	fun contextLoads() {
		println("Stream bridge:$streamBridge")
		println("Persona service:$personaService")
		println("Compania service:$companiaService")
		println("JwtDecoder:$reactiveJwtDecoder")
	}

	/**
	 * This TestConfiguration is for ALL file testers, so do not delete this class.
	 *
	 * This is to configure the ObjectMapper with JSR310Module and Java 8 JavaTime()
	 * module that it is not initialized for test mode. i.e., ObjectMapper @Autowired does not exist
	 */
	@TestConfiguration
	class ObjectMapperConfiguration {
		@Bean
		fun objectMapper(): ObjectMapper = ObjectMapper().findAndRegisterModules()
	}
}
