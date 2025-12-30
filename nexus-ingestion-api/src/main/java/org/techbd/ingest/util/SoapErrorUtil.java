package org.techbd.ingest.util;

import org.springframework.ws.soap.SoapFault;
import org.springframework.ws.soap.SoapFaultDetail;
import org.springframework.ws.soap.SoapMessage;
import org.techbd.ingest.exceptions.ErrorTraceIdGenerator;
import org.techbd.ingest.exceptions.SoapFaultResponse;

import javax.xml.namespace.QName;

/**
 * Utility class for creating structured SOAP error responses.
 * Provides consistent error formatting with trace IDs for PIX/PNR endpoints.
 */
public class SoapErrorUtil {

    private SoapErrorUtil() {
        // Utility class
    }

    /**
     * Creates a structured SOAP fault response with error trace ID.
     */
    public static SoapFaultResponse createSoapFaultResponse(
            String faultCode, 
            String faultString, 
            int errorCode, 
            String errorMessage, 
            String interactionId) {
        
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        // Log the error for debugging
        LogUtil.logDetailedError(errorCode, errorMessage, interactionId, errorTraceId, null);
        
        SoapFaultResponse.ErrorInfo errorInfo = new SoapFaultResponse.ErrorInfo(
            errorCode,
            errorMessage,
            interactionId != null ? interactionId : "unknown",
            errorTraceId
        );
        
        SoapFaultResponse.SoapFaultDetail detail = new SoapFaultResponse.SoapFaultDetail(errorInfo);
        SoapFaultResponse.SoapFault fault = new SoapFaultResponse.SoapFault(faultCode, faultString, detail);
        
        return new SoapFaultResponse(fault);
    }

    /**
     * Creates a server error SOAP fault response.
     */
    public static SoapFaultResponse createServerErrorResponse(String interactionId, Exception ex) {
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        // Log detailed error
        LogUtil.logDetailedError(500, "Internal server error", interactionId, errorTraceId, ex);
        
        SoapFaultResponse.ErrorInfo errorInfo = new SoapFaultResponse.ErrorInfo(
            500,
            "Internal server error",
            interactionId != null ? interactionId : "unknown",
            errorTraceId
        );
        
        SoapFaultResponse.SoapFaultDetail detail = new SoapFaultResponse.SoapFaultDetail(errorInfo);
        SoapFaultResponse.SoapFault fault = new SoapFaultResponse.SoapFault(
            "soap:Server", 
            "Internal server error occurred", 
            detail
        );
        
        return new SoapFaultResponse(fault);
    }

    /**
     * Creates a client error SOAP fault response for validation errors.
     */
    public static SoapFaultResponse createClientErrorResponse(String interactionId, String validationError) {
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        // Log validation error
        LogUtil.logDetailedError(400, "Validation error: " + validationError, interactionId, errorTraceId, null);
        
        SoapFaultResponse.ErrorInfo errorInfo = new SoapFaultResponse.ErrorInfo(
            400,
            "Bad Request",
            interactionId != null ? interactionId : "unknown",
            errorTraceId
        );
        
        SoapFaultResponse.SoapFaultDetail detail = new SoapFaultResponse.SoapFaultDetail(errorInfo);
        SoapFaultResponse.SoapFault fault = new SoapFaultResponse.SoapFault(
            "soap:Client", 
            "Validation error: " + validationError, 
            detail
        );
        
        return new SoapFaultResponse(fault);
    }

    /**
     * Creates a Spring WS SOAP fault with structured error details.
     */
    public static SoapFault createSpringWsSoapFault(SoapMessage soapMessage, Exception ex, String interactionId) {
        String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
        
        SoapFault soapFault = soapMessage.getSoapBody().addServerOrReceiverFault(
            "Internal server error occurred", 
            java.util.Locale.ENGLISH
        );
        
        SoapFaultDetail faultDetail = soapFault.addFaultDetail();
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "errorTraceId"))
                   .addText(errorTraceId);
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "interactionId"))
                   .addText(interactionId != null ? interactionId : "unknown");
        faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "code"))
                   .addText("500");
        
        LogUtil.logDetailedError(500, "SOAP fault occurred", interactionId, errorTraceId, ex);
        
        return soapFault;
    }
}