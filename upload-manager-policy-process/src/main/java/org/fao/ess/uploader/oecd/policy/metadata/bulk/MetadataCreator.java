package org.fao.ess.uploader.oecd.policy.metadata.bulk;

import org.fao.fenix.commons.msd.dto.full.*;
import org.fao.fenix.commons.msd.dto.type.DocumentType;
import org.fao.fenix.commons.msd.dto.type.RepresentationType;
import org.fao.fenix.commons.msd.dto.type.ResponsiblePartyRole;
import org.fao.fenix.commons.utils.FileUtils;
import org.fao.fenix.commons.utils.JSONUtils;

import javax.inject.Inject;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MetadataCreator {
    @Inject protected FileUtils fileUtils;

    private String[] header;

    public void setHeader(String[] header) {
        this.header = header;
    }

    private static final String templatePath = "policy/oecd/metadata/templates/";
    public DSDDataset create (String templateName) throws Exception {
        InputStream templateStream = this.getClass().getResourceAsStream(templatePath+templateName+".json");
        if (templateStream==null)
            return null;
        String templateContent = fileUtils.readTextFile(templateStream);
        return JSONUtils.toObject(templateContent, DSDDataset.class);
    }

    public MeIdentification<DSDDataset> create(String[] csvRecord) throws Exception {
        Collection<String> errors = new LinkedList<>();
        Map<String, String> record = toMap(header, csvRecord);
        MeIdentification<DSDDataset> metadata = new MeIdentification<>();

        metadata.setUid(record.get("uid"));
        metadata.setTitle(toLabel(record.get("title")));
        metadata.setLanguage(toOjCodeList("ISO639-2", "1998", record.get("language")));
        metadata.setMetadataLanguage(toOjCodeList("ISO639-2", "1998", record.get("metadataLanguage")));
        metadata.setLanguageDetails(toLabel(record.get("languageDetails")));
        try {
            metadata.setCreationDate(toDate(record.get("creationDate")));
        } catch (Exception ex) {
            errors.add("Wrong creation date format");
        }
        metadata.setCharacterSet(toOjCodeList("IANAcharacterSet", null, record.get("characterSet")));
        metadata.setMetadataStandardName(record.get("metadataStandardName"));
        metadata.setMetadataStandardVersion(record.get("metadataStandardVersion"));
        try {
            OjResponsibleParty contact = toContact(
                    record.get("pointOfContact"),
                    record.get("organization"),
                    record.get("organizationUnit"),
                    record.get("position"),
                    record.get("role"),
                    record.get("specify"), null, null,
                    record.get("emailAddress"), null, null
            );
            if (contact != null)
                metadata.setContacts(Arrays.asList(contact));
        } catch (Exception ex) {
            errors.add(ex.getMessage());
        }
        MeContent meContent = new MeContent();
        meContent.setResourceRepresentationType(RepresentationType.dataset);
        String keyWords = record.get("keywords");
        meContent.setKeywords(keyWords != null && keyWords.trim().length() > 0 ? Arrays.asList(keyWords.split(",")) : null);
        meContent.setDescription(toLabel(record.get("description")));
        meContent.setStatisticalConceptsDefinitions(toLabel(record.get("statisticalConceptsDefinitions")));
        metadata.setMeContent(meContent);

        SeCoverage seCoverage = new SeCoverage();
        seCoverage.setCoverageSectors(toOjCodeList("CRS_purpose_codes", null, record.get("coverageSectors")));
        try {
            seCoverage.setCoverageTime(toPeriod(record.get("from"), record.get("to")));
        } catch (Exception ex) {
            errors.add("Wrong 'from' or 'to' date format");
        }
        seCoverage.setCoverageGeographic(toOjCodeList("GAUL", "2014", record.get("coverageGeographic")));
        meContent.setSeCoverage(seCoverage);

        SeReferencePopulation seReferencePopulation = new SeReferencePopulation();
        seReferencePopulation.setStatisticalPopulation(toLabel(record.get("statisticalPopulation")));
        seReferencePopulation.setStatisticalUnit(toLabel(record.get("statisticalUnit")));
        seReferencePopulation.setReferencePeriod(toOjCodeList("FAO_Period", "1.0", record.get("referencePeriod")));
        seReferencePopulation.setReferenceArea(toOjCodeList("GAUL_ReferenceArea", "1.0", record.get("referenceArea")));
        meContent.setSeReferencePopulation(seReferencePopulation);

        MeInstitutionalMandate meInstitutionalMandate = new MeInstitutionalMandate();
        meInstitutionalMandate.setLegalActsAgreements(toLabel(record.get("legalActsAgreements")));
        meInstitutionalMandate.setInstitutionalMandateDataSharing(toLabel(record.get("institutionalMandateDataSharing")));
        metadata.setMeInstitutionalMandate(meInstitutionalMandate);

        MeDataQuality meDataQuality = new MeDataQuality();
        metadata.setMeDataQuality(meDataQuality);

        SeComparability seComparability = new SeComparability();
        seComparability.setCoherenceIntern(toLabel(record.get("coherenceIntern")));
        meDataQuality.setSeComparability(seComparability);

        MeStatisticalProcessing meStatisticalProcessing = new MeStatisticalProcessing();
        metadata.setMeStatisticalProcessing(meStatisticalProcessing);

        SeDataSource seDataSource = new SeDataSource();
        meStatisticalProcessing.setSeDataSource(seDataSource);

        SePrimaryDataCollection sePrimaryDataCollection = new SePrimaryDataCollection();
        sePrimaryDataCollection.setDataCollection(toLabel(record.get("dataCollection"))); //TODO
        sePrimaryDataCollection.setCollectionPeriodicity(toOjCodeList("FAO_Period", "1.0", record.get("collectionPeriodicity")));
        seDataSource.setSePrimaryDataCollection(sePrimaryDataCollection);

        SeSecondaryDataCollection seSecondaryDataCollection = new SeSecondaryDataCollection();
        seSecondaryDataCollection.setOriginOfCollectedData(toOjCodeList("FAOSTAT_OriginData", "1.0", record.get("originOfCollectedData")));
        seSecondaryDataCollection.setDataCollection(toLabel(record.get("dataCollection"))); //TODO
        seSecondaryDataCollection.setOrganization(toLabel(record.get("Organization")));
        seDataSource.setSeSecondaryDataCollection(seSecondaryDataCollection);

        SeDataCompilation seDataCompilation = new SeDataCompilation();
        seDataCompilation.setDataAdjustmentDetails(toLabel(record.get("rawDataDescription")));
        meStatisticalProcessing.setSeDataCompilation(seDataCompilation);

        MeAccessibility meAccessibility = new MeAccessibility();
        metadata.setMeAccessibility(meAccessibility);

        SeDataDissemination seDataDissemination = new SeDataDissemination();
        meAccessibility.setSeDataDissemination(seDataDissemination);

        SeDistribution seDistribution = new SeDistribution();
        seDistribution.setOnlineResource(record.get("onlineResource"));
        String dissemniationFormat = record.get("disseminationFormat");
        seDistribution.setDisseminationFormat(dissemniationFormat != null ? Arrays.asList(dissemniationFormat) : null);
        seDataDissemination.setSeDistribution(seDistribution);

        SeReleasePolicy seReleasePolicy = new SeReleasePolicy();
        seReleasePolicy.setDisseminationPeriodicity(toOjCodeList("FAO_Period", "1.0", record.get("disseminationPeriodicity")));
        seDataDissemination.setSeReleasePolicy(seReleasePolicy);

        SeConfidentiality seConfidentiality = new SeConfidentiality();
        seConfidentiality.setConfidentialityStatus(toOjCodeList("CL_CONF_STATUS", "1.0", record.get("confidentialityStatus")));
        meAccessibility.setSeConfidentiality(seConfidentiality);

        MeMaintenance meMaintenance = new MeMaintenance();
        meMaintenance.setMaintenanceAgency(toLabel(record.get("maintenanceAgency")));
        metadata.setMeMaintenance(meMaintenance);

        SeUpdate seUpdate = new SeUpdate();
        seUpdate.setUpdatePeriodicity(toOjCodeList("FAO_Period", "1.0", record.get("updatePeriodicity")));
        meMaintenance.setSeUpdate(seUpdate);

        SeMetadataMaintenance seMetadataMaintenance = new SeMetadataMaintenance();
        try {
            seMetadataMaintenance.setMetadataLastUpdate(toDate(record.get("metadataLastUpdate")));
        } catch (Exception ex) {
            errors.add("Wrong metadata last update date format");
        }
        meMaintenance.setSeMetadataMaintenance(seMetadataMaintenance);

        Collection<MeDocuments> documents = new LinkedList<>();
        for (int i = 1; i <= 6; i++) {
            try {
                MeDocuments document = toDocument(
                        record.get("documentKind_0" + i),
                        record.get("title_0" + i),
                        record.get("date_0" + i),
                        record.get("notes_0" + i),
                        record.get("link_0" + i),
                        record.get("pointOfContact_0" + i),
                        record.get("organization_0" + i),
                        record.get("organizationUnit_0" + i)
                );
                if (document != null)
                    documents.add(document);
            } catch (Exception ex) {
                errors.add("Errors in document " + i + ":\n");
            }
        }

        removeEmpty(metadata);
        validate(metadata, errors);

        if (errors.size() > 0) {
            StringBuilder message = new StringBuilder();
            for (String error : errors)
                message.append(error).append('\n');
            throw new Exception(message.toString());
        } else
            return metadata;
    }

    private void removeEmpty (MeIdentification<DSDDataset> metadata) {
        //TODO
    }

    private void validate (MeIdentification<DSDDataset> metadata, Collection<String> errors) {
        //TODO
    }

    private Map<String, String> toMap(String[] header, String[] record) {
        Map<String, String> mapRecord = new HashMap<>();
        for (int i=0; i<header.length && i<record.length; i++) {
            record[i] =  record[i]!=null ? record[i].trim() : null;
            mapRecord.put(header[i].trim(), record[i]!=null && record[i].length()>0 ? record[i] : null);
        }
        return mapRecord;
    }

    private MeDocuments toDocument (String documentKind, String title, String date, String notes, String link, String pointOfContact, String organization, String organizationUnit) throws Exception {
        if (
                (documentKind==null || documentKind.trim().length()==0) &&
                        (title==null || title.trim().length()==0) &&
                        (date==null || date.trim().length()==0) &&
                        (notes==null || notes.trim().length()==0) &&
                        (link==null || link.trim().length()==0) &&
                        (pointOfContact==null || pointOfContact.trim().length()==0) &&
                        (organization==null || organization.trim().length()==0) &&
                        (organizationUnit==null || organizationUnit.trim().length()==0) )
            return null;

        Collection<String> errors = new LinkedList<>();

        OjCitation document = new OjCitation();
        DocumentType type = documentKind!=null && documentKind.trim().length()>0 ? DocumentType.valueOf(documentKind) : null;
        if (type==null && documentKind!=null && documentKind.trim().length()>0)
            errors.add("Wrong document kind");
        document.setDocumentKind(type);
        document.setTitle(toLabel(title));
        try { document.setDate(toDate(date)); } catch (ParseException ex) { errors.add("Wrong date format"); }
        document.setNotes(toLabel(notes));
        document.setLink(link);
        try {
            document.setDocumentContact(toContact(pointOfContact,organization, organizationUnit, null, null, null, null, null, null, null, null));
        } catch (Exception ex) {
            errors.add(ex.getMessage());
        }

        if (errors.size()>0) {
            StringBuilder message = new StringBuilder();
            for (String error : errors)
                message.append(error).append('\n');
            throw new Exception(message.toString());
        } else {
            MeDocuments meDocuments = new MeDocuments();
            meDocuments.setDocument(document);
            return meDocuments;
        }
    }

    private Map<String,String> toLabel(String label, String ... language) {
        if (label==null)
            return null;
        Map<String,String> mapLabel = new HashMap<>();
        mapLabel.put(language!=null && language.length>0 ? language[0] : "EN", label);
        return mapLabel;
    }

    private OjCodeList toOjCodeList (String uid, String version, String ... codes) {
        Collection<String> codesList = new LinkedList<>();
        if (codes!=null)
            for (String code : codes)
                if (code!=null)
                    codesList.add(code);
        if (codesList.size()==0)
            return null;

        OjCodeList oj = new OjCodeList();
        oj.setIdCodeList(uid);
        oj.setVersion(version);
        Collection<OjCode> codesOj = new LinkedList<>();
        for (String code : codesList) {
            OjCode codeOj = new OjCode();
            codeOj.setCode(code);
            codesOj.add(codeOj);
        }
        oj.setCodes(codesOj);

        return oj;
    }

    private static final DateFormat format = new SimpleDateFormat("dd-MM-yyyy");
    private Date toDate(String dateString) throws ParseException {
        return dateString!=null && dateString.trim().length()>0 ? format.parse(dateString) : null;
    }

    private OjPeriod toPeriod(String from, String to) throws ParseException {
        Date fromDate = toDate(from);
        Date toDate = toDate(to);
        if (fromDate!=null || toDate!=null) {
            OjPeriod period = new OjPeriod();
            period.setFrom(fromDate);
            period.setTo(toDate);
            return period;
        } else
            return null;
    }

    private OjResponsibleParty toContact (
            String pointOfContact,
            String organization,
            String organizationUnit,
            String position,
            String role,
            String specify,
            String phone,
            String address,
            String emailAddress,
            String hoursOfService,
            String contactIntruction
    ) throws Exception {

        if (
                (pointOfContact==null || pointOfContact.trim().length()==0) &&
                        (organization==null || organization.trim().length()==0) &&
                        (organizationUnit==null || organizationUnit.trim().length()==0) &&
                        (position==null || position.trim().length()==0) &&
                        (role==null || role.trim().length()==0) &&
                        (specify==null || specify.trim().length()==0) &&
                        (phone==null || phone.trim().length()==0) &&
                        (address==null || address.trim().length()==0) &&
                        (emailAddress==null || emailAddress.trim().length()==0) &&
                        (hoursOfService==null || hoursOfService.trim().length()==0) &&
                        (contactIntruction==null || contactIntruction.trim().length()==0)   )
            return null;

        ResponsiblePartyRole responsiblePartyRole = null;
        if (role!=null && role.trim().length()>0 && (responsiblePartyRole = ResponsiblePartyRole.valueOf(role))==null)
            throw new Exception("Undefined contact role: "+role);

        OjResponsibleParty responsibleParty = new OjResponsibleParty();
        responsibleParty.setPointOfContact(pointOfContact);
        responsibleParty.setOrganization(toLabel(organization));
        responsibleParty.setOrganizationUnit(toLabel(organizationUnit));
        responsibleParty.setPosition(toLabel(position));
        responsibleParty.setRole(responsiblePartyRole);
        responsibleParty.setSpecify(toLabel(specify));
        responsibleParty.setPointOfContact(pointOfContact);

        if (
                (phone!=null && phone.trim().length()>0) ||
                        (address!=null && address.trim().length()>0) ||
                        (emailAddress!=null && emailAddress.trim().length()>0) ||
                        (hoursOfService!=null && hoursOfService.trim().length()>0) ||
                        (contactIntruction!=null && contactIntruction.trim().length()>0)   ) {
            OjContact contactInfo = new OjContact();
            contactInfo.setPhone(phone);
            contactInfo.setAddress(address);
            contactInfo.setEmailAddress(emailAddress);
            contactInfo.setHoursOfService(toLabel(hoursOfService));
            contactInfo.setContactInstruction(toLabel(contactIntruction));
            responsibleParty.setContactInfo(contactInfo);
        }

        return responsibleParty;
    }

}
