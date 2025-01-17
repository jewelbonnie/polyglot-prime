package org.techbd.orchestrate.csv;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import org.techbd.model.csv.FileDetail;
import org.techbd.service.http.hub.prime.AppConfig;
import org.techbd.service.http.hub.prime.AppConfig.Csvs;

import reactor.core.publisher.Mono;

// Implementation for Python Service (WebClient)
@Component
@Qualifier("pythonService")
public class PythonServiceValidationStrategy implements CsvValidationStrategy {
    private static final Logger log = LoggerFactory.getLogger(PythonExecutorValidationStrategy.class);

    private final AppConfig appConfig;

    public PythonServiceValidationStrategy(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

 @Override
    public String validateCsv(List<FileDetail> fileDetails, String interactionId) {
      
       log.info("CsvService : validateCsvUsingPython BEGIN for interactionId : {}", interactionId);

            try {
                // Validate input
                if (fileDetails == null || fileDetails.isEmpty()) {
                    log.error("No files provided for validation");
                    throw new IllegalArgumentException("No files provided for validation");
                }

                // Fetch CSV configuration from AppConfig
                final Csvs cs = appConfig.getCsvs();
                final var baseUrl = cs.baseUrl();
                final var endpoint = cs.endpoint();

                // Log URL information for debugging
                log.info("Using baseUrl: {}, endpoint: {}", baseUrl, endpoint);

                // Create WebClient instance
                final WebClient webClient = WebClient.builder()
                        .baseUrl(baseUrl)
                        .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE)
                        .build();

                // Prepare multipart form data
                final MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
                final Set<String> requiredFields = new HashSet<>(Arrays.asList(
                        "QE_ADMIN_DATA_FILE",
                        "SCREENING_PROFILE_DATA_FILE",
                        "SCREENING_OBSERVATION_DATA_FILE",
                        "DEMOGRAPHIC_DATA_FILE"));

                for (final FileDetail fileDetail : fileDetails) {
                    final var fieldName = determineFieldName(fileDetail.filename());
                    if (fieldName != null) {
                        final byte[] fileBytes = fileDetail.content().getBytes(StandardCharsets.UTF_8);
                        formData.add(fieldName, new ByteArrayResource(fileBytes) {
                            @Override
                            public String getFilename() {
                                return fileDetail.filename();
                            }
                        });
                        requiredFields.remove(fieldName);
                    }
                }

                if (!requiredFields.isEmpty()) {
                    final var missingFields = String.join(", ", requiredFields);
                    log.error("Missing required files: {}", missingFields);
                    throw new IllegalArgumentException("Missing required files: " + missingFields);
                }

                //POST request with streaming response handling
                final var response = webClient.post()
                        .uri(endpoint)
                        .bodyValue(formData)
                        .retrieve()
                        .onStatus(HttpStatusCode::isError, clientResponse -> clientResponse.bodyToMono(String.class)
                                .flatMap(errorBody -> Mono.error(new RuntimeException(
                                        "Validation failed with status code: " +
                                                clientResponse.statusCode() + " and body: " + errorBody))))
                        .bodyToFlux(String.class)
                        .collect(StringBuilder::new, StringBuilder::append)
                        .map(StringBuilder::toString)
                        .block();

                log.info("CsvService : validateCsvUsingPython END for interactionId : {}", interactionId);
                return response;

            } catch (final Exception e) {
                log.error("Error during CSV validation: {}", e.getMessage(), e);
                throw new RuntimeException("Error during CSV validation", e);
            }
        }

        private String determineFieldName(final String filename) {
            if (filename.startsWith("QE_ADMIN_DATA_")) {
                return "QE_ADMIN_DATA_FILE";
            } else if (filename.startsWith("SCREENING_PROFILE_DATA_")) {
                return "SCREENING_PROFILE_DATA_FILE";
            } else if (filename.startsWith("SCREENING_OBSERVATION_DATA_")) {
                return "SCREENING_OBSERVATION_DATA_FILE";
            } else if (filename.startsWith("DEMOGRAPHIC_DATA_")) {
                return "DEMOGRAPHIC_DATA_FILE";
            }
            log.warn("Unrecognized filename pattern: {}", filename);
            return null;
        }
    }














