package org.techbd.ingest.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.ws.soap.server.endpoint.SoapFaultMappingExceptionResolver;
import org.springframework.ws.soap.server.endpoint.SoapFaultDefinition;
import org.springframework.ws.soap.SoapFault;
import org.springframework.ws.soap.SoapFaultDetail;
import org.springframework.ws.transport.context.TransportContextHolder;
import org.springframework.ws.transport.http.HttpServletConnection;
import org.techbd.ingest.commons.Constants;
import org.techbd.ingest.exceptions.ErrorTraceIdGenerator;
import org.techbd.ingest.util.LogUtil;

import javax.xml.namespace.QName;
import java.util.Properties;

/**
 * Configuration for SOAP fault handling with error trace IDs.
 * Provides global exception handling for SOAP endpoints.
 */
@Configuration
public class SoapFaultConfig {

    @Bean
    public SoapFaultMappingExceptionResolver exceptionResolver() {
        SoapFaultMappingExceptionResolver exceptionResolver = new SoapFaultMappingExceptionResolver() {
            @Override
            protected void customizeFault(Object endpoint, Exception ex, SoapFault fault) {
                String errorTraceId = ErrorTraceIdGenerator.generateErrorTraceId();
                String interactionId = extractInteractionId();
                
                // Add error trace ID to fault detail
                SoapFaultDetail faultDetail = fault.getFaultDetail();
                if (faultDetail == null) {
                    faultDetail = fault.addFaultDetail();
                }
                
                faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "errorTraceId"))
                           .addText(errorTraceId);
                faultDetail.addFaultDetailElement(new QName("http://techbd.org/soap/fault", "interactionId"))
                           .addText(interactionId != null ? interactionId : "unknown");
                
                // Log detailed error
                LogUtil.logDetailedError(500, "SOAP fault: " + fault.getFaultStringOrReason(), 
                                       interactionId, errorTraceId, ex);
                
                super.customizeFault(endpoint, ex, fault);
            }
            
            private String extractInteractionId() {
                // Extract interaction ID from current request context
                try {
                    var transportContext = TransportContextHolder.getTransportContext();
                    if (transportContext != null) {
                        var connection = (HttpServletConnection) transportContext.getConnection();
                        var httpRequest = connection.getHttpServletRequest();
                        
                        String interactionId = httpRequest.getHeader(Constants.HEADER_INTERACTION_ID);
                        if (interactionId == null) {
                            interactionId = (String) httpRequest.getAttribute(Constants.INTERACTION_ID);
                        }
                        return interactionId;
                    }
                } catch (Exception e) {
                    // Fallback to unknown if extraction fails
                }
                return "unknown";
            }
        };
        
        // Configure exception mappings
        Properties errorMappings = new Properties();
        errorMappings.setProperty("java.lang.Exception", "SERVER");
        errorMappings.setProperty("java.lang.RuntimeException", "SERVER");
        errorMappings.setProperty("java.lang.IllegalArgumentException", "CLIENT");
        
        exceptionResolver.setExceptionMappings(errorMappings);
        
        SoapFaultDefinition defaultFault = new SoapFaultDefinition();
        defaultFault.setFaultCode(SoapFaultDefinition.SERVER);
        exceptionResolver.setDefaultFault(defaultFault);
        exceptionResolver.setOrder(1);
        
        return exceptionResolver;
    }
}