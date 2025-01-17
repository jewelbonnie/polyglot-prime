package org.techbd.orchestrate.csv;

import java.util.List;

import org.techbd.model.csv.FileDetail;

public interface CsvValidationStrategy {
 
    String validateCsv(List<FileDetail> fileDetails, String interactionId);

}
