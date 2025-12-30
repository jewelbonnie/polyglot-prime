package org.techbd.ingest.exceptions;

/**
 * SOAP fault response structure for PIX/PNR ingestion API.
 * Contains error information with trace ID for debugging.
 */
public record SoapFaultResponse(
    SoapFault fault
) {
    public record SoapFault(
        String faultcode,
        String faultstring,
        SoapFaultDetail detail
    ) {}
    
    public record SoapFaultDetail(
        ErrorInfo errorInfo
    ) {}
    
    public record ErrorInfo(
        int code,
        String message,
        String interactionId,
        String errorTraceId
    ) {}
}