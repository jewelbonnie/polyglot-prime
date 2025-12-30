package org.techbd.ingest.exceptions;

import org.springframework.ws.soap.SoapFault;
import org.springframework.ws.soap.SoapFaultDetail;
import org.springframework.ws.soap.SoapMessage;
import org.springframework.ws.soap.server.endpoint.SoapFaultMappingExceptionResolver;
import org.techbd.ingest.util.LogUtil;

import javax.xml.namespace.QName;

/**
 * SOAP fault handler for PIX/PNR endpoints.
 * Generates structured SOAP faults with error trace IDs for better debugging.
 */
public class SoapFaultHandler extends SoapFaultMappingExceptionResolver {

    /**
     * Creates a SOAP fault with error trace ID and structured error information.
     */
    public static SoapFault createSoapFault(SoapMessage soapMessage, Exception ex, String interactionId) {
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        // Create SOAP fault
        SoapFault soapFault = soapMessage.getSoapBody().addServerOrReceiverFault(
            "Internal server error occurred", 
            java.util.Locale.ENGLISH
        );
        
        // Add fault detail with error trace ID
        SoapFaultDetail faultDetail = soapFault.addFaultDetail();
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "errorTraceId"))
                   .addText(errorTraceId);
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "interactionId"))
                   .addText(interactionId != null ? interactionId : "unknown");
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "code"))
                   .addText("500");
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "message"))
                   .addText("Internal server error");
        
        // Log detailed error for debugging
        LogUtil.logDetailedError(500, "SOAP fault occurred", interactionId, errorTraceId, ex);
        
        return soapFault;
    }
    
    /**
     * Creates a SOAP fault for validation errors.
     */
    public static SoapFault createValidationSoapFault(SoapMessage soapMessage, String validationError, String interactionId) {
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        SoapFault soapFault = soapMessage.getSoapBody().addClientOrSenderFault(
            "Validation error: " + validationError, 
            java.util.Locale.ENGLISH
        );
        
        SoapFaultDetail faultDetail = soapFault.addFaultDetail();
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "errorTraceId"))
                   .addText(errorTraceId);
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "interactionId"))
                   .addText(interactionId != null ? interactionId : "unknown");
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "code"))
                   .addText("400");
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "message"))
                   .addText("Bad Request");
        
        // Log validation error
        LogUtil.logDetailedError(400, "SOAP validation error: " + validationError, interactionId, errorTraceId, null);
        
        return soapFault;
    }
}