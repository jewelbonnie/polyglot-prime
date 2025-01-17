package org.techbd.orchestrate.csv;

import org.apache.commons.vfs2.FileObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.techbd.model.csv.FileDetail;
import org.techbd.model.csv.FileType;
import org.techbd.service.VfsCoreService;
import org.techbd.service.http.hub.prime.AppConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

// Implementation for Python Executor (ProcessBuilder)
@Component
@Qualifier("pythonExecutor")
public class PythonExecutorValidationStrategy implements CsvValidationStrategy {
    private static final Logger log = LoggerFactory.getLogger(PythonExecutorValidationStrategy.class);
    private static final int PROCESS_TIMEOUT_SECONDS = 300; // 5 minutes timeout
    private static final int EXPECTED_COMMAND_SIZE = 7;

    private final AppConfig appConfig;
    private final VfsCoreService vfsCoreService;

    public PythonExecutorValidationStrategy(AppConfig appConfig, VfsCoreService vfsCoreService) {
        this.appConfig = appConfig;
        this.vfsCoreService = vfsCoreService;
    }

    @Override
    public String validateCsv(List<FileDetail> fileDetails, String interactionId) {
        log.info("Starting CSV validation for interactionId: {}", interactionId);
        
        validateInput(fileDetails);
        final var config = getValidationConfig();
        
        try {
            List<FileObject> fileObjects = validateFiles(fileDetails);
            List<String> command = buildValidationCommand(config, fileDetails);
            
            return executePythonScript(command, fileDetails.get(0).filePath(), interactionId);
        } catch (IOException | InterruptedException e) {
            log.error("Error during CSV validation for interactionId {}: {}", interactionId, e.getMessage(), e);
            throw new RuntimeException("CSV validation failed", e);
        }
    }

    private void validateInput(List<FileDetail> fileDetails) {
        if (fileDetails == null || fileDetails.isEmpty()) {
            throw new IllegalArgumentException("No files provided for validation");
        }
    }

    private AppConfig.CsvValidation.Validation getValidationConfig() {
        final var config = appConfig.getCsv().validation();
        if (config == null) {
            throw new IllegalStateException("CSV validation configuration is null");
        }
        return config;
    }

    private List<FileObject> validateFiles(List<FileDetail> fileDetails) throws IOException {
        List<FileObject> fileObjects = new ArrayList<>();
        for (FileDetail fileDetail : fileDetails) {
            log.debug("Validating file: {}", fileDetail);
            FileObject file = vfsCoreService.resolveFile(fileDetail.filePath());
            if (!vfsCoreService.fileExists(file)) {
                throw new IOException("File not found: " + fileDetail.filePath());
            }
            fileObjects.add(file);
        }
        vfsCoreService.validateAndCreateDirectories(fileObjects.toArray(new FileObject[0]));
        return fileObjects;
    }

    private String executePythonScript(List<String> command, String workingDirectory, String interactionId) 
            throws IOException, InterruptedException {
        log.info("Executing validation command: {}", String.join(" ", command));
        
        ProcessBuilder processBuilder = new ProcessBuilder(command)
            .directory(new File(workingDirectory).getParentFile())
            .redirectErrorStream(true);

        Process process = processBuilder.start();
        StringBuilder output = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                log.debug("Process output: {}", line);
                output.append(line).append("\n");
            }
        }

        if (!process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IOException("Python script execution timed out after " + PROCESS_TIMEOUT_SECONDS + " seconds");
        }

        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new IOException("Python script execution failed with exit code " + exitCode);
        }

        log.info("CSV validation completed successfully for interactionId: {}", interactionId);
        return output.toString().trim();
    }

    private List<String> buildValidationCommand(AppConfig.CsvValidation.Validation config, List<FileDetail> fileDetails) {
        List<String> command = new ArrayList<>();
        command.add(config.pythonExecutable());
        command.add("validate-nyher-fhir-ig-equivalent.py");
        command.add("datapackage-nyher-fhir-ig-equivalent.json");

        Map<FileType, String> fileTypeToFileNameMap = fileDetails.stream()
            .collect(Collectors.toMap(FileDetail::fileType, FileDetail::filename));

        Arrays.asList(
            FileType.QE_ADMIN_DATA,
            FileType.SCREENING_PROFILE_DATA,
            FileType.SCREENING_OBSERVATION_DATA,
            FileType.DEMOGRAPHIC_DATA
        ).forEach(fileType -> command.add(fileTypeToFileNameMap.getOrDefault(fileType, "")));

        while (command.size() < EXPECTED_COMMAND_SIZE) {
            command.add("");
        }

        return command;
    }
}
