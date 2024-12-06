
package org.techbd.tasklet;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.techbd.commonvfsservice.VfsCoreService;
import org.techbd.customexception.IncompleteGroupException;
//import org.techbd.service.CsvValidationService;
import org.techbd.javapythonintegration.CsvValidationService;
import lib.aide.vfs.VfsIngressConsumer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A Spring Batch tasklet implementation that processes and validates ZIP files containing
 * demographic, QE admin, and screening data CSV files.
 */
@Slf4j
@Component  
public class ZipProcessingTasklet implements Tasklet {
    
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "(DEMOGRAPHIC_DATA|QE_ADMIN_DATA|SCREENING)_(.+)");

    @Value("${path.inbound.folder}")
    private String inboundFolder;

    @Value("${path.ingress.home}")
    private String ingressHome;

    //@Value("${validation.output.path}")
    private String validationOutputPath ="/home/jewel/workspaces/github.com/jewelbonnie/polyglot-prime/hub-prime/src/main/java/org/techbd/javapythonintegration/output/output.json";

    private final VfsCoreService vfsCoreService;
    private final CsvValidationService csvValidationService;
    private final ObjectMapper objectMapper;

    /**
     * Constructs a new ZipProcessingTasklet with required dependencies.
     *
     * @param vfsCoreService Service for VFS operations
     * @param csvValidationService Service for CSV validation
     * @param objectMapper JSON object mapper
     */
    @Autowired
    public ZipProcessingTasklet(
            VfsCoreService vfsCoreService,
            CsvValidationService csvValidationService,
            ObjectMapper objectMapper) {
        this.vfsCoreService = vfsCoreService;
        this.csvValidationService = csvValidationService;
        this.objectMapper = objectMapper;
    }

    /**
     * Executes the tasklet, processing and validating ZIP files.
     *
     * @param contribution Mutable state to be passed back to the JobRepository
     * @param chunkContext Contains information from the job and step environment
     * @return RepeatStatus indicating whether processing should repeat
     * @throws Exception if any error occurs during processing
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        try {
            // Process ZIP files and get the session ID
            UUID processId = processZipFiles();
            log.info("ZIP files processed with session ID: {}", processId);

            // Get processed files for validation
            //FileObject processedDir = vfsCoreService.resolveFile(ingressHome + "/" + processId);
            FileObject processedDir = vfsCoreService.resolveFile(ingressHome + "/" + processId + "/ingress");
            if (!vfsCoreService.fileExists(processedDir)) {
                throw new FileSystemException("Processed directory not found: " + processedDir.getName().getPath());
            }

            // Collect CSV files for validation
            List<String> csvFiles = collectCsvFiles(processedDir);
            log.info("Found {} CSV files for validation", csvFiles.size());

            // Validate CSV files
            Map<String, Object> validationResults = validateFiles(csvFiles);
            System.out.println("Validation Results: " + validationResults);
            // Save validation results
           // saveValidationResults(processId, validationResults);
            
            return RepeatStatus.FINISHED;
        } catch (Exception e) {
            log.error("Error in ZIP processing tasklet: {}", e.getMessage(), e);
            throw new RuntimeException("Error processing ZIP files: " + e.getMessage(), e);
        }
    }

    /**
     * Processes ZIP files from the inbound directory.
     *
     * @return UUID representing the processing session
     * @throws FileSystemException if there are any file system related errors
     */
    private UUID processZipFiles() throws FileSystemException {
        FileObject inboundFO = vfsCoreService.resolveFile(inboundFolder);
        FileObject ingresshomeFO = vfsCoreService.resolveFile(ingressHome);
       
        if (!vfsCoreService.fileExists(inboundFO)) {
            throw new FileSystemException("Inbound folder does not exist: " + inboundFO.getName().getPath());
        }
        vfsCoreService.validateAndCreateDirectories(ingresshomeFO);

        VfsIngressConsumer consumer = vfsCoreService.createConsumer(
                inboundFO,
                this::extractGroupId,
                this::isGroupComplete
        );
        return vfsCoreService.processFiles(consumer, ingresshomeFO);
    }

    /**
     * Collects paths of CSV files from the processed directory.
     *
     * @param processedDir Directory containing processed files
     * @return List of CSV file paths
     * @throws FileSystemException if there's an error accessing files
     */
    private List<String> collectCsvFiles(FileObject processedDir) throws FileSystemException {
        List<String> csvFiles = new ArrayList<>();
        FileObject[] children = processedDir.getChildren();
        for (FileObject child : children) {
            if (child.getName().getExtension().equalsIgnoreCase("csv")) {
                csvFiles.add(child.getName().getPath());
            }
        }
        return csvFiles;
    }


    // /**
//      * Validates a group of CSV files after processing
//      *
//      * @param group The group of files to validate
//      * @return ValidationResult containing validation status and errors
//       * @throws Exception 
//           */
//          private Map<String, Object> validateGroup(VfsIngressConsumer.IngressGroup group) throws Exception {
//         // Extract paths of CSV files from the group
//         List<String> csvPaths = group.groupedEntries().stream()
//             .map(entry -> entry.entry().getName().getPath())
//             .collect(Collectors.toList());
            
//         // Validate using Python service
//         return csvValidationService.validateCsvGroup(csvPaths);
//     }

    // /**
    //  * Validates a collection of CSV files.
    //  *
    //  * @param csvFiles List of CSV file paths to validate
    //  * @return Map containing validation results
    //  */
    // private Map<String, Object> validateFiles(List<String> csvFiles) {
    //     try {
    //         return csvValidationService.validateCsvGroup(csvFiles);
    //     } catch (Exception e) {
    //         log.error("Error validating CSV files: {}", e.getMessage(), e);
    //         Map<String, Object> errorResult = new HashMap<>();
    //         errorResult.put("validationError", e.getMessage());
    //         errorResult.put("status", "FAILED");
    //         return errorResult;
    //     }
    // }

    private Map<String, Object> validateFiles(List<String> csvFiles) {
        Map<String, Object> validationResults = new HashMap<>();
        
        // Group files by test case number
        Map<String, List<String>> groupedFiles = csvFiles.stream()
            .collect(Collectors.groupingBy(filePath -> {
                // Extract test case number from file path
                String fileName = Paths.get(filePath).getFileName().toString();
                // Extract the testcase number using regex
                Pattern pattern = Pattern.compile(".*-testcase(\\d+)\\.csv$");
                var matcher = pattern.matcher(fileName);
                if (matcher.find()) {
                    return matcher.group(1); // Returns the test case number
                }
                return "unknown";
            }));
    
        // Process each group together
        for (Map.Entry<String, List<String>> entry : groupedFiles.entrySet()) {
            String testCaseNum = entry.getKey();
            List<String> group = entry.getValue();
            
            try {
                log.debug("Starting CSV validation for test case {}: {}", testCaseNum, group);
                Map<String, Object> groupResults = csvValidationService.validateCsvGroup(group);
                validationResults.put("testcase_" + testCaseNum, groupResults);
                log.debug("Validation results for test case {}: {}", testCaseNum, groupResults);
            } catch (Exception e) {
                log.error("Error validating CSV files for test case {}: {}", testCaseNum, e.getMessage(), e);
                Map<String, Object> errorResult = new HashMap<>();
                errorResult.put("validationError", e.getMessage());
                errorResult.put("status", "FAILED");
                validationResults.put("testcase_" + testCaseNum, errorResult);
            }
        }
    
        return validationResults;
    }


    //*********working code but resoving random group file issue********* */
    // private Map<String, Object> validateFiles(List<String> csvFiles) {
    //     int batchSize = 3; // or any other suitable batch size
    //     Map<String, Object> validationResults = new HashMap<>();
    
    //     for (int i = 0; i < csvFiles.size(); i += batchSize) {
    //         List<String> batch = csvFiles.subList(i, Math.min(i + batchSize, csvFiles.size()));
    //         try {
    //             log.debug("Starting CSV validation for batch: {}", batch);
    //             Map<String, Object> batchResults = csvValidationService.validateCsvGroup(batch);
    //             validationResults.putAll(batchResults);
    //             log.debug("Validation results for batch: {}", batchResults);
    //         } catch (Exception e) {
    //             log.error("Error validating CSV files: {}", e.getMessage(), e);
    //             Map<String, Object> errorResult = new HashMap<>();
    //             errorResult.put("validationError", e.getMessage());
    //             errorResult.put("status", "FAILED");
    //             validationResults.put("batch_" + (i / batchSize), errorResult);
    //         }
    //     }
    
    //     return validationResults;
    // }
    //*********working*** */
    // /**
    //  * Saves validation results to a JSON file.
    //  *
    //  * @param processId Session ID for the current processing
    //  * @param results Validation results to save
    //  */
    // private void saveValidationResults(UUID processId, Map<String, Object> results) {
    //     try {
    //         Path outputPath = Paths.get(validationOutputPath, processId + "_validation.json");
    //         objectMapper.writeValue(outputPath.toFile(), results);
    //         log.info("Validation results saved to: {}", outputPath);
    //     } catch (Exception e) {
    //         log.error("Error saving validation results: {}", e.getMessage(), e);
    //         throw new RuntimeException("Failed to save validation results", e);
    //     }
    // }
/**
 * Saves validation results to a JSON file.
 *
 * @param processId Session ID for the current processing
 * @param results Validation results to save
 */
// private void saveValidationResults(UUID processId, Map<String, Object> results) {
//     try {
//         // Create the output directory if it doesn't exist
//         Path outputDir = Paths.get(validationOutputPath);
//         if (!Files.exists(outputDir)) {
//             Files.createDirectories(outputDir);
//             log.info("Created output directory: {}", outputDir);
//         }

//         // Create the output file path
//         Path outputFile = outputDir.resolve(processId + "_validation.json");
        
//         // Write the results to the file
//         objectMapper.writeValue(outputFile.toFile(), results);
//         log.info("Validation results saved to: {}", outputFile);
        
//     } catch (IOException e) {
//         log.error("Error saving validation results: {}", e.getMessage(), e);
//         throw new RuntimeException("Failed to save validation results: " + e.getMessage(), e);
//     }
// }

    private String extractGroupId(FileObject file) {
        String fileName = file.getName().getBaseName();
        var matcher = FILE_PATTERN.matcher(fileName);
        return matcher.matches() ? matcher.group(2) : null;
    }

    private boolean isGroupComplete(VfsIngressConsumer.IngressGroup group) {
        if (group == null || group.groupedEntries().isEmpty()) {
            return false;
        }

        boolean hasDemographic = false;
        boolean hasQeAdmin = false;
        boolean hasScreening = false;

        for (VfsIngressConsumer.IngressIndividual entry : group.groupedEntries()) {
            String fileName = entry.entry().getName().getBaseName();
            if (fileName.startsWith("DEMOGRAPHIC_DATA_")) {
                hasDemographic = true;
            } else if (fileName.startsWith("QE_ADMIN_DATA_")) {
                hasQeAdmin = true;
            } else if (fileName.startsWith("SCREENING_")) {
                hasScreening = true;
            }
        }

        if (!(hasDemographic && hasQeAdmin && hasScreening)) {
            String groupId = group.groupedEntries().isEmpty() ? "unknown"
                    : extractGroupId(group.groupedEntries().get(0).entry());
            throw new IncompleteGroupException(groupId, hasDemographic, hasQeAdmin, hasScreening);
        }
        return true;
    }
}



// package org.techbd.tasklet;

// import org.apache.commons.vfs2.FileObject;
// import org.apache.commons.vfs2.FileSystemException;
// import org.springframework.batch.core.StepContribution;
// import org.springframework.batch.core.scope.context.ChunkContext;
// import org.springframework.batch.core.step.tasklet.Tasklet;
// import org.springframework.batch.repeat.RepeatStatus;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.beans.factory.annotation.Value;
// import org.springframework.stereotype.Component;
// import org.techbd.commonvfsservice.VfsCoreService;
// import org.techbd.customexception.IncompleteGroupException;
// import org.techbd.javapythonintegration.CsvValidationService;

// import lib.aide.vfs.VfsIngressConsumer;

// import java.util.List;
// import java.util.Map;
// import java.util.UUID;
// import java.util.regex.Pattern;
// import java.util.stream.Collectors;

// /**
//  * A Spring Batch tasklet implementation that processes ZIP files containing
//  * demographic,
//  * QE admin, and screening data. This tasklet validates and processes grouped
//  * files
//  * according to specific naming patterns and completeness requirements.
//  */
// @Component
// public class ZipProcessingTasklet implements Tasklet {
//     private static final Pattern FILE_PATTERN = Pattern.compile(
//             "(DEMOGRAPHIC_DATA|QE_ADMIN_DATA|SCREENING)_(.+)");

//     @Value("${path.inbound.folder}")
//     private String inboundFolder;

//     @Value("${path.ingress.home}")
//     private String ingressHome;

//     private final VfsCoreService vfsCoreService;
//     private final CsvValidationService csvValidationService;

//     @Autowired
//     public ZipProcessingTasklet(VfsCoreService vfsCoreService,CsvValidationService csvValidationService) {
//         this.vfsCoreService = vfsCoreService;
//         this.csvValidationService = csvValidationService;
//     }

//     /**
//      * Executes the tasklet, processing ZIP files in the inbound directory.
//      *
//      * @param contribution Mutable state to be passed back to the JobRepository
//      * @param chunkContext Contains information from the job and step environment
//      * @return RepeatStatus indicating whether processing should repeat
//      * @throws Exception if any error occurs during processing
//      */
//     @Override
//     public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//         try {
//             processZipFiles();
            
//             return RepeatStatus.FINISHED;
//         } catch (Exception e) {
//             throw new RuntimeException("Error processing ZIP files: " + e.getMessage(), e);
//         }
//     }

//     /**
//      * Processes ZIP files from the inbound directory, validating and moving them
//      * to the ingress home directory.
//      *
//      * @return UUID representing the processing session
//      * @throws FileSystemException if there are any file system related errors
//      */
//     private UUID processZipFiles() throws FileSystemException {
//         FileObject inboundFO = vfsCoreService.resolveFile(inboundFolder);
//         FileObject ingresshomeFO = vfsCoreService.resolveFile(ingressHome);

//         // Validate directories
//         if (!vfsCoreService.fileExists(inboundFO)) {
//             throw new FileSystemException("Inbound folder does not exist: " + inboundFO.getName().getPath());
//         }
//         vfsCoreService.validateAndCreateDirectories(ingresshomeFO);

//         // Create VFS consumer with grouping and validation
//         VfsIngressConsumer consumer = vfsCoreService.createConsumer(
//                 inboundFO,
//                 this::extractGroupId,
//                 this::isGroupComplete);

//         return vfsCoreService.processFiles(consumer, ingresshomeFO);
//     }

//     /**
//      * Extracts the group ID from a file name based on the defined pattern.
//      *
//      * @param file The file object to extract the group ID from
//      * @return The extracted group ID, or null if the file name doesn't match the
//      *         pattern
//      */
//     private String extractGroupId(FileObject file) {
//         String fileName = file.getName().getBaseName();
//         var matcher = FILE_PATTERN.matcher(fileName);
//         return matcher.matches() ? matcher.group(2) : null;
//     }

//     /**
//      * Validates whether a group of files is complete by checking for the presence
//      * of all required file types (demographic, QE admin, and screening data).
//      *
//      * @param group The group of files to validate
//      * @return true if the group is complete, false otherwise
//      * @throws IncompleteGroupException if the group is missing required file types
//      */
//     private boolean isGroupComplete(VfsIngressConsumer.IngressGroup group) {
//         if (group == null || group.groupedEntries().isEmpty()) {
//             return false;
//         }

//         boolean hasDemographic = true;
//         boolean hasQeAdmin = true;
//         boolean hasScreening = true;

//         for (VfsIngressConsumer.IngressIndividual entry : group.groupedEntries()) {
//             String fileName = entry.entry().getName().getBaseName();
//             if (fileName.startsWith("DEMOGRAPHIC_DATA_")) {
//                 hasDemographic = true;
//             } else if (fileName.startsWith("QE_ADMIN_DATA_")) {
//                 hasQeAdmin = true;
//             } else if (fileName.startsWith("SCREENING_")) {
//                 hasScreening = true;
//             }
//         }

//         if (!(hasDemographic && hasQeAdmin && hasScreening)) {
//             String groupId = group.groupedEntries().isEmpty() ? "unknown"
//                     : extractGroupId(group.groupedEntries().get(0).entry());
//             throw new IncompleteGroupException(groupId, hasDemographic, hasQeAdmin, hasScreening);
//         }

//         return true;
//     }

// /**
//      * Validates a group of CSV files after processing
//      *
//      * @param group The group of files to validate
//      * @return ValidationResult containing validation status and errors
//       * @throws Exception 
//           */
//          private Map<String, Object> validateGroup(VfsIngressConsumer.IngressGroup group) throws Exception {
//         // Extract paths of CSV files from the group
//         List<String> csvPaths = group.groupedEntries().stream()
//             .map(entry -> entry.entry().getName().getPath())
//             .collect(Collectors.toList());
            
//         // Validate using Python service
//         return csvValidationService.validateCsvGroup(csvPaths);
//     }


// }
