package org.techbd.ingest.util;

import org.springframework.ws.soap.SoapFaultException;
import org.springframework.ws.soap.SoapMessage;

/**
 * Utility for SOAP request validation with structured fault responses.
 */
public class SoapValidationUtil {

    private SoapValidationUtil() {
        // Utility class
    }

    /**
     * Validates required fields and throws SOAP fault if validation fails.
     */
    public static void validateRequired(Object value, String fieldName, String interactionId, SoapMessage soapMessage) {
        if (value == null) {
            SoapErrorUtil.createSpringWsSoapFault(
                soapMessage, 
                new IllegalArgumentException("Missing required field: " + fieldName), 
                interactionId
            );
            throw new SoapFaultException("Missing required field: " + fieldName);
        }
    }

    /**
     * Validates string is not empty and throws SOAP fault if validation fails.
     */
    public static void validateNotEmpty(String value, String fieldName, String interactionId, SoapMessage soapMessage) {
        if (value == null || value.trim().isEmpty()) {
            SoapErrorUtil.createSpringWsSoapFault(
                soapMessage, 
                new IllegalArgumentException("Field cannot be empty: " + fieldName), 
                interactionId
            );
            throw new SoapFaultException("Field cannot be empty: " + fieldName);
        }
    }
}