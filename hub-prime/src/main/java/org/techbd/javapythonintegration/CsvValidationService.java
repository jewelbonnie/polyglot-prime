package org.techbd.javapythonintegration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Service
@Slf4j
public class CsvValidationService {
    private final ObjectMapper objectMapper;
    private String pythonScriptPath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/validate_csvs.py";
    private String pythonExecutable = "python3";
    private String packagePath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/datapackage-ig.json";
    private String outputPath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/output/output.json";
    
    // Base path to strip from CSV file paths
    private static final String BASE_PATH = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/";

    public CsvValidationService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    private String convertToRelativePath(String fullPath) {
        if (fullPath.startsWith(BASE_PATH)) {
            return fullPath.substring(BASE_PATH.length());
        }
        return fullPath;
    }

    public Map<String, Object> validateCsvGroup(List<String> csvFiles) throws Exception {
        try {
            if (csvFiles.size() != 3) {
                throw new IllegalArgumentException("Must provide exactly 3 CSV files for validation");
            }

            // Convert CSV paths to relative paths
            List<String> relativeCsvPaths = csvFiles.stream()
                .map(this::convertToRelativePath)
                .collect(Collectors.toList());
            
            // Build command
            List<String> command = new ArrayList<>();
            command.add(pythonExecutable);
            command.add(pythonScriptPath);
            command.add(packagePath);
            command.addAll(relativeCsvPaths);
            command.add(outputPath);

            log.info("Executing command: {}", String.join(" ", command));

            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            
            // Set working directory to the project root
            processBuilder.directory(new File(BASE_PATH));

            Process process = processBuilder.start();

            // Read output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    log.debug("Python output: {}", line);
                }
            }

            // Read error stream separately
            StringBuilder errorOutput = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    errorOutput.append(line).append("\n");
                    log.error("Python error: {}", line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                String errorMessage = String.format(
                        "Python validation script failed with exit code: %d\nOutput: %s\nError: %s",
                        exitCode, output, errorOutput);
                log.error(errorMessage);
                throw new Exception(errorMessage);
            }

            log.debug("Python script output: {}", output);
            
            //Map<String, Object> result = objectMapper.readValue(output.toString(), new TypeReference<Map<String, Object>>() {});
           var result = output.toString();
            log.info("Python validation script executed successfully :"+result);
            return Map.of("result",result);
            
        // } catch (JsonProcessingException e) {
        //     log.error("Error parsing Python script output: {}", e.getMessage(), e);
        //     throw new Exception("Failed to parse validation results", e);
        } catch (Exception e) {
            log.error("Error validating CSV files: {}", e.getMessage(), e);
            throw new Exception("Failed to validate CSV files", e);
        }
    }
}
//*******************************working code************************************************************************** */
// @Service
// @Slf4j
// public class CsvValidationService {
//     private final ObjectMapper objectMapper;

//     private String pythonScriptPath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/validate_csvs.py";
//     private String pythonExecutable = "python3";
//     private String csvDataDir = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/data";
//     String packagePath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/datapackage-ig.json";
//     private String outputPath = "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/output/output.json";

//     public CsvValidationService(ObjectMapper objectMapper) {
//         this.objectMapper = objectMapper;
//     }

//     public Map<String, Object> validateCsvGroup(List<String> csvFiles) throws Exception {
//         try {
//             if (csvFiles.size() != 3) {
//                 throw new IllegalArgumentException("Must provide exactly 3 CSV files for validation");
//             }
        
//            // String outputPath = validationOutputPath + "/validation_" + UUID.randomUUID() + ".json";
            
//             // Build command
//             List<String> command = new ArrayList<>();
//             command.add(pythonExecutable);
//             command.add(pythonScriptPath);
//             command.add(packagePath);
//             command.addAll(csvFiles);
//             command.add(outputPath);
//             // command.add(csvDataDir);

//             // Execute Python script
//             ProcessBuilder processBuilder = new ProcessBuilder(command.toArray(new String[0]));
//             processBuilder.redirectErrorStream(true); // Merge stderr into stdout
//             // *change** */
//             // // Set the environment variable
//             // Map<String, String> environment = processBuilder.environment();
//             // environment.put("CSV_DATA_DIR",
//             //         "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/data/");

//             log.info("Executing command: {}", String.join(" ", command));

//             Process process = processBuilder.start();

//             // Read output
//             StringBuilder output = new StringBuilder();
//             try (BufferedReader reader = new BufferedReader(
//                     new InputStreamReader(process.getInputStream()))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     output.append(line).append("\n");
//                     log.debug("Python output: {}", line);
//                 }
//             }

//             // Read error stream separately
//             StringBuilder errorOutput = new StringBuilder();
//             try (BufferedReader reader = new BufferedReader(
//                     new InputStreamReader(process.getErrorStream()))) {
//                 String line;
//                 while ((line = reader.readLine()) != null) {
//                     errorOutput.append(line).append("\n");
//                     log.error("Python error: {}", line);
//                 }
//             }

//             // Check exit code
//             int exitCode = process.waitFor();
//             if (exitCode != 0) {
//                 String errorMessage = String.format(
//                         "Python validation script failed with exit code: %d\nOutput: %s\nError: %s",
//                         exitCode, output, errorOutput);
//                 log.error(errorMessage);
//                 throw new Exception(errorMessage);
//             }

//         //     log.info("Python validation script executed successfully");
//         //     return objectMapper.readValue(output.toString(), Map.class);

//         // } catch (Exception e) {
//         //     log.error("Error validating CSV files: {}", e.getMessage(), e);
//         //     throw new Exception("Failed to validate CSV files", e);
//         // }
//         log.debug("Python script output: {}", output);
            
//         // Parse JSON response
//         Map<String, Object> result = objectMapper.readValue(output.toString(), new TypeReference<Map<String, Object>>() {});
//         log.info("Python validation script executed successfully");
//         return result;
        
//     } catch (JsonProcessingException e) {
//         log.error("Error parsing Python script output: {}", e.getMessage(), e);
//         throw new Exception("Failed to parse validation results", e);
//     } catch (Exception e) {
//         log.error("Error validating CSV files: {}", e.getMessage(), e);
//         throw new Exception("Failed to validate CSV files", e);
//     }
    
//     }
// }
//********************end******************************************************* */
// package org.techbd.javapythonintegration;

// import com.fasterxml.jackson.databind.ObjectMapper;

// import lombok.extern.slf4j.Slf4j;

// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Service;
// import java.io.BufferedReader;
// import java.io.InputStreamReader;
// import java.util.ArrayList;
// import java.util.List;
// import java.util.Map;
// import java.util.logging.Logger;

// /**
// * Service for validating CSV files using Python Frictionless framework
// */
// @Service
// @Slf4j
// public class CsvValidationService {

// private final ObjectMapper objectMapper;

// // @Value("${python.script.path}")
// private String pythonScriptPath =
// "/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/validate_csvs.py";

// //@Value("${python.executable}")
// private String pythonExecutable = "python3" ;

// public CsvValidationService(ObjectMapper objectMapper) {
// this.objectMapper = objectMapper;
// }

// /**
// * Validates a group of CSV files using the Python Frictionless framework
// *
// * @param csvFiles List of paths to CSV files to validate
// * @return Map containing validation results
// * @throws ValidationException if validation process fails
// */
// public Map<String, Object> validateCsvGroup(List<String> csvFiles) throws
// Exception {
// try {
// // Build command
// List<String> command = new ArrayList<>();
// command.add(pythonExecutable);
// command.add(pythonScriptPath);
// command.addAll(csvFiles);
// //command.toArray();
// // Execute Python script
// ProcessBuilder processBuilder = new ProcessBuilder(command.toArray(new
// String[0]));
// Process process = processBuilder.start();

// // Read output
// StringBuilder output = new StringBuilder();
// try (BufferedReader reader = new BufferedReader(
// new InputStreamReader(process.getInputStream()))) {
// String line;
// while ((line = reader.readLine()) != null) {
// output.append(line).append("\n");
// }
// }

// // Check exit code
// int exitCode = process.waitFor();
// if (exitCode != 0) {
// throw new Exception("Python validation script failed with exit code: " +
// exitCode);
// }else{
// log.info("Python validation script executed successfully."+output);
// System.out.println("Python validation script executed
// successfully...."+output);

// }

// // if (exitCode == 0) {
// // logger.info("Python script executed successfully.");
// // } else {
// // logger.severe("Python script executed with errors. Exit code: " +
// exitCode);
// // }

// // Parse JSON output
// return objectMapper.readValue(output.toString(), Map.class);

// } catch (Exception e) {
// log.error("Error validating CSV files: {}", e.getMessage(), e);
// throw new Exception("Failed to validate CSV files", e);
// }
// }
// }