package org.techbd.service.converters.shinny;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Identifier;
import org.hl7.fhir.r4.model.Meta;
import org.hl7.fhir.r4.model.Organization;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.techbd.model.csv.DemographicData;
import org.techbd.model.csv.QeAdminData;
import org.techbd.model.csv.ScreeningObservationData;
import org.techbd.model.csv.ScreeningProfileData;
import org.techbd.util.CsvConstants;
import org.techbd.util.CsvConversionUtil;
import org.techbd.util.DateUtil;

/**
 * Converts data related to an Organization into a FHIR Organization resource.
 */
@Component
@Order(1)
public class OrganizationConverter extends BaseConverter {
    private static final Logger LOG = LoggerFactory.getLogger(OrganizationConverter.class.getName());

    /**
     * Returns the resource type associated with this converter.
     *
     * @return The FHIR ResourceType.Organization enum.
     */
    public ResourceType getResourceType() {
        return ResourceType.Organization;
    }

    /**
     * Converts organization-related data into a FHIR Organization resource
     * wrapped in a BundleEntryComponent.
     *
     * @param bundle            The FHIR Bundle to which the organization data is
     *                          related.
     * @param demographicData   The demographic data related to the patient (for
     *                          organization details).
     * @param qrAdminData       The administrative data related to the patient or
     *                          organization.
     * @param screeningDataList The list of screening data (if required for the
     *                          organization context).
     * @param interactionId     The interaction ID used for tracking or referencing
     *                          the conversion.
     * @return A BundleEntryComponent containing the converted FHIR Organization
     *         resource.
     */
    @Override
    public List<BundleEntryComponent>  convert(Bundle bundle,DemographicData demographicData,QeAdminData qeAdminData ,
    ScreeningProfileData screeningProfileData ,List<ScreeningObservationData> screeningObservationData,String interactionId,Map<String,String> idsGenerated) {
        Organization organization = new Organization();
        setMeta(organization);
        organization.setId(CsvConversionUtil.sha256(qeAdminData.getFacilityId())); // Assuming qrAdminData contains orgId
        idsGenerated.put(CsvConstants.ORGANIZATION_ID,organization.getId());
        String fullUrl = "http://shinny.org/us/ny/hrsn/Organization/" + organization.getId();
        Meta meta = organization.getMeta();
        meta.setLastUpdated(DateUtil.parseDate(qeAdminData.getFacilityLastUpdated()));
        populateOrganizationName(organization, qeAdminData);
        populateOrganizationIdentifier(organization, qeAdminData);
        populateIsActive(organization, qeAdminData);
        populateOrganizationType(organization, qeAdminData);
        populateOrganizationAddress(organization, qeAdminData);
        BundleEntryComponent bundleEntryComponent = new BundleEntryComponent();
        bundleEntryComponent.setFullUrl(fullUrl);
        bundleEntryComponent.setRequest(new Bundle.BundleEntryRequestComponent().setMethod(HTTPVerb.POST).setUrl("http://shinny.org/us/ny/hrsn/Organization/" + organization.getId()));
        bundleEntryComponent.setResource(organization);
        return List.of(bundleEntryComponent);
    }

    private static void populateOrganizationName(Organization organization, QeAdminData qrAdminData) {
        if (qrAdminData.getFacilityName() != null) {
            organization.setName(qrAdminData.getFacilityName());
        }
    }

    private static void populateOrganizationIdentifier(Organization organization, QeAdminData data) {
        if (StringUtils.isNotEmpty(data.getFacilityId())) {
            Identifier identifier = new Identifier();
            Coding coding = new Coding();

            if (StringUtils.isNotEmpty(data.getFacilityIdentifierTypeDisplay())) {
                coding.setDisplay(data.getFacilityIdentifierTypeDisplay());
                LOG.info("Coding Display Set: {}", data.getFacilityIdentifierTypeDisplay());
            }

            if (StringUtils.isNotEmpty(data.getFacilityIdentifierTypeValue())) {
                identifier.setValue(data.getFacilityIdentifierTypeValue());
                LOG.info("Identifier Value Set: {}", data.getFacilityIdentifierTypeValue());
            }

            if (StringUtils.isNotEmpty(data.getFacilityIdentifierTypeSystem())) {
                identifier.setSystem(data.getFacilityIdentifierTypeSystem());
                LOG.info("Adding Identifier: Type Display - {}, Value - {}", coding.getDisplay(), identifier.getValue());
            }

            CodeableConcept type = new CodeableConcept();
            type.addCoding(coding);
            identifier.setType(type);
            organization.addIdentifier(identifier);
        }
    }

    private static void populateOrganizationType(Organization organization, QeAdminData data) {
        if (StringUtils.isNotEmpty(data.getOrganizationTypeCode()) || StringUtils.isNotEmpty(data.getOrganizationTypeDisplay())) {
            CodeableConcept type = new CodeableConcept();

            // Create a new Coding object
            Coding coding = new Coding();
            String system = "http://terminology.hl7.org/CodeSystem/organization-type";  //TODO : remove static reference
            coding.setSystem(system);
            coding.setCode(data.getOrganizationTypeCode());
            coding.setDisplay(data.getOrganizationTypeDisplay());

            type.addCoding(coding);

            organization.setType(Collections.singletonList(type));
        }
    }

    private static void populateOrganizationAddress(Organization organization, QeAdminData qrAdminData) {
        if (StringUtils.isNotEmpty(qrAdminData.getFacilityAddress1()) || StringUtils.isNotEmpty(qrAdminData.getFacilityCity()) ||
            StringUtils.isNotEmpty(qrAdminData.getFacilityState()) || StringUtils.isNotEmpty(qrAdminData.getFacilityZip())) {

            Address address = new Address();

            String fullAddressText = qrAdminData.getFacilityAddress1();
            if (StringUtils.isNotEmpty(qrAdminData.getFacilityCity())) {
                fullAddressText += ", " + qrAdminData.getFacilityCity();
            }
            if (StringUtils.isNotEmpty(qrAdminData.getFacilityState())) {
                fullAddressText += ", " + qrAdminData.getFacilityState();
            }
            if (StringUtils.isNotEmpty(qrAdminData.getFacilityZip())) {
                fullAddressText += " " + qrAdminData.getFacilityZip();
            }
            address.setText(fullAddressText);

            if (StringUtils.isNotEmpty(qrAdminData.getFacilityAddress1())){
                List<StringType> addressLines = new ArrayList<>();
                addressLines.add(new StringType(qrAdminData.getFacilityAddress1()));
                address.setLine(addressLines);
            }

            address.setCity(qrAdminData.getFacilityCity());
            address.setDistrict(qrAdminData.getFacilityDistrict());
            address.setState(qrAdminData.getFacilityState());
            address.setPostalCode(qrAdminData.getFacilityZip());

            organization.addAddress(address);
        }
    }

    private static void populateIsActive(Organization organization, QeAdminData qrAdminData) {
        if (StringUtils.isNotEmpty("TRUE")) {  //TODO : remove static reference
            organization.setActive(Boolean.parseBoolean("TRUE"));
        }
    }
}
