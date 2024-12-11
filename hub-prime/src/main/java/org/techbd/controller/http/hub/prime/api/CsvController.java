package org.techbd.controller.http.hub.prime.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.techbd.conf.Configuration;
import org.techbd.service.CsvService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

//@RestController
//public class CsvController {
    //@PostMapping("/test-upload")
//      @PostMapping(value = "/flatfile/csv/Bundle/$validate", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)

//     public ResponseEntity<String> testUpload(@RequestParam("file") MultipartFile file) {
//        String fileName = file.getOriginalFilename();
//        System.out.println("File name: " + fileName + "fileSize is : "+ file.getSize());
//       if (file.isEmpty()) {
//             return ResponseEntity.badRequest().body("File is empty");
//         }
//         return ResponseEntity.ok("File uploaded successfully: " + file.getOriginalFilename());
//     }
// }



@RestController
@Tag(name = "Tech by Design Hub CSV Endpoints", description = "Tech by Design Hub CSV Endpoints")
public class CsvController {
  private static final Logger log = LoggerFactory.getLogger(CsvController.class);
  private final CsvService csvService;

  public CsvController(CsvService csvService) {
    this.csvService = csvService;
  }

  @PostMapping(value = "/flatfile/csv/Bundle/$validate", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  @Operation(summary = "Endpoint to upload, process, and validate a CSV ZIP file", description = "Upload a ZIP file containing CSVs for processing and validation.")
  @ApiResponses({
      @ApiResponse(responseCode = "200", description = "CSV files processed successfully", content = @Content(mediaType = "application/json", examples = @ExampleObject(value = """
          {
            "status": "Success",
            "message": "CSV files processed successfully."
          }
          """))),
      @ApiResponse(responseCode = "400", description = "Validation Error: Missing or invalid parameter", content = @Content(mediaType = "application/json", examples = {
          @ExampleObject(value = """
              {
                "status": "Error",
                "message": "Validation Error: Required request body is missing."
              }
              """),
          @ExampleObject(value = """
              {
                "status": "Error",
                "message": "Validation Error: Required request header 'X-Tenant-ID' is missing."
              }
              """)
      })),
      @ApiResponse(responseCode = "500", description = "An unexpected system error occurred", content = @Content(mediaType = "application/json", examples = @ExampleObject(value = """
          {
            "status": "Error",
            "message": "An unexpected system error occurred."
          }
          """)))
  })
  @ResponseBody
  public Object handleCsvUpload(
      @Parameter(description = "ZIP file containing CSV data. Must not be null.", required = true) @RequestPart("file") @Nonnull MultipartFile file,
      @Parameter(description = "Tenant ID, a mandatory parameter.", required = true) @RequestHeader(value = Configuration.Servlet.HeaderName.Request.TENANT_ID) String tenantId,
      HttpServletRequest request,
      HttpServletResponse response)
      throws Exception {
    if (tenantId == null || tenantId.trim().isEmpty()) {
      log.error("CsvController: handleCsvUpload:: Tenant ID is missing or empty");
      throw new IllegalArgumentException("Tenant ID must be provided");
    }
    // long fileSize = file.getSize();
    // System.out.println("*****fileSize is "+fileSize);
    String fileName = file.getOriginalFilename();
    log.info("CsvController: handleCsvUpload:: Uploaded file name is {}", fileName);
    System.out.println("*****fileName is " + fileName);

    long fileSize = file.getSize(); // Get the file size in bytes
    log.info("CsvController: handleCsvUpload:: Received file of size {} bytes", fileSize);
    System.out.println("*****fileSize is " + fileSize);
    // Ensure file size is logged or acted upon if needed (e.g., max size check)
    if (fileSize == 0) {
      log.error("CsvController: handleCsvUpload:: Uploaded file is empty");
      throw new IllegalArgumentException("Uploaded file cannot be empty");
    }

    return csvService.validateCsvFile(file, request, response, tenantId);
  }
}
