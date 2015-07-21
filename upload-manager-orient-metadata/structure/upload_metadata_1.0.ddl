CONNECT remote:localhost/msd_2.0 admin admin;


CREATE CLASS MeIdentification;

CREATE CLASS MeDocuments;

CREATE CLASS MeInstitutionalMandate;

CREATE CLASS MeAccessibility;
  CREATE CLASS SeDataDissemination;
    CREATE CLASS SeDistribution;
    CREATE CLASS SeReleasePolicy;
  CREATE CLASS SeClarity;
  CREATE CLASS SeConfidentiality;

CREATE CLASS MeContent;
  CREATE CLASS SeReferencePopulation;
  CREATE CLASS SeCoverage;
  CREATE CLASS SeCodeList;

CREATE CLASS MeDataQuality;
  CREATE CLASS SeAccuracy;
  CREATE CLASS SeDataRevision;
  CREATE CLASS SeRelevance;
  CREATE CLASS SeComparability;
  CREATE CLASS SeTimelinessPunctuality;

CREATE CLASS MeMaintenance;
  CREATE CLASS SeUpdate;
  CREATE CLASS SeMetadataMaintenance;


CREATE CLASS MeReferenceSystem;
  CREATE CLASS SeProjection;
    CREATE CLASS SeProjectionParameters;
      CREATE CLASS SeObliqueLineAzimuth;
      CREATE CLASS SeObliqueLinePoint;
  CREATE CLASS SeEllipsoid;
    CREATE CLASS SeEllipsoidParameters;
  CREATE CLASS SeDatum;


CREATE CLASS MeResourceStructure;
  CREATE CLASS SeResourceDimensions;
  CREATE CLASS SeResourceRecords;

CREATE CLASS MeSpatialRepresentation;
  CREATE CLASS SeBoundingBox;
    CREATE CLASS SeGridSpatialRepresentation;
    CREATE CLASS SeVectorSpatialRepresentation;


CREATE CLASS MeStatisticalProcessing;
  CREATE CLASS SeDataSource;
    CREATE CLASS SePrimaryDataCollection;
    CREATE CLASS SeSecondaryDataCollection;
  CREATE CLASS SeDataCompilation;
  CREATE CLASS SeDataValidation;


CREATE CLASS OjResponsibleParty;
CREATE CLASS OjContact;
CREATE CLASS OjCitation;
CREATE CLASS OjCodeList;
CREATE CLASS OjCode;
CREATE CLASS OjMeasure;
CREATE CLASS OjAxis;
CREATE CLASS OjPeriod;

CREATE CLASS Period;
CREATE CLASS Code;

CREATE CLASS DSD;
CREATE CLASS DSDDataset EXTENDS DSD;
CREATE CLASS DSDGeographic EXTENDS DSD;
CREATE CLASS DSDCodelist EXTENDS DSD;
CREATE CLASS DSDDocument EXTENDS DSD;
CREATE CLASS DSDAggregationRule;
CREATE CLASS DSDColumn;
CREATE CLASS DSDColumnSubject;
CREATE CLASS DSDDomain;





CREATE PROPERTY Period.from LONG;
CREATE PROPERTY Period.to LONG;

CREATE PROPERTY OjPeriod.from DATE;
CREATE PROPERTY OjPeriod.to DATE;

CREATE PROPERTY OjCodeList.linkedCodeList LINK MeIdentification;
CREATE PROPERTY OjCodeList.idCodeList STRING;
CREATE PROPERTY OjCodeList.version STRING;
CREATE PROPERTY OjCodeList.codes EMBEDDEDLIST OjCode;
CREATE PROPERTY OjCodeList.extendedName EMBEDDEDMAP STRING;
CREATE PROPERTY OjCodeList.contactInfo EMBEDDED OjResponsibleParty;
CREATE PROPERTY OjCodeList.codeListResources EMBEDDEDLIST OjCitation;
CREATE PROPERTY OjCodeList.link STRING;

CREATE PROPERTY OjCode.linkedCode LINK Code;
CREATE PROPERTY OjCode.code STRING;
CREATE PROPERTY OjCode.label EMBEDDEDMAP STRING;

CREATE PROPERTY OjMeasure.extent EMBEDDEDMAP STRING;
CREATE PROPERTY OjMeasure.composedMU BOOLEAN;
CREATE PROPERTY OjMeasure.measurementSystem EMBEDDEDMAP STRING;
CREATE PROPERTY OjMeasure.nameMU STRING;
CREATE PROPERTY OjMeasure.conversionToStandard DOUBLE;

CREATE PROPERTY OjResponsibleParty.organization EMBEDDEDMAP STRING;
CREATE PROPERTY OjResponsibleParty.organizationUnit EMBEDDEDMAP STRING;
CREATE PROPERTY OjResponsibleParty.pointOfContact STRING;
CREATE PROPERTY OjResponsibleParty.position EMBEDDEDMAP STRING;
CREATE PROPERTY OjResponsibleParty.role STRING;
CREATE PROPERTY OjResponsibleParty.specify EMBEDDEDMAP STRING;
CREATE PROPERTY OjResponsibleParty.contactInfo EMBEDDED OjContact;

CREATE PROPERTY OjContact.phone STRING;
CREATE PROPERTY OjContact.address STRING;
CREATE PROPERTY OjContact.emailAddress STRING;
CREATE PROPERTY OjContact.hoursOfService EMBEDDEDMAP STRING;
CREATE PROPERTY OjContact.contactInstruction EMBEDDEDMAP STRING;

CREATE PROPERTY OjCitation.documentKind STRING;
CREATE PROPERTY OjCitation.title EMBEDDEDMAP STRING;
CREATE PROPERTY OjCitation.date DATE;
CREATE PROPERTY OjCitation.documentContact EMBEDDED OjResponsibleParty;
CREATE PROPERTY OjCitation.notes EMBEDDEDMAP STRING;
CREATE PROPERTY OjCitation.link STRING;
CREATE PROPERTY OjCitation.isbn STRING;
CREATE PROPERTY OjCitation.issn STRING;

CREATE PROPERTY OjAxis.axisName STRING;
CREATE PROPERTY OjAxis.axisSize INTEGER;
CREATE PROPERTY OjAxis.resolution EMBEDDED OjMeasure;





CREATE PROPERTY DSDColumnSubject.name STRING;
CREATE PROPERTY DSDColumnSubject.link STRING;
CREATE PROPERTY DSDColumnSubject.title EMBEDDEDMAP STRING;

CREATE PROPERTY DSD.contextExtension EMBEDDEDMAP;
CREATE PROPERTY DSD.contextSystem STRING;
CREATE PROPERTY DSD.datasources EMBEDDEDLIST STRING;

CREATE PROPERTY DSDGeographic.workspace STRING;
CREATE PROPERTY DSDGeographic.layerName STRING;

CREATE PROPERTY DSDDataset.aggregationRules LINKLIST DSDAggregationRule;
CREATE PROPERTY DSDDataset.columns EMBEDDEDLIST DSDColumn;

CREATE PROPERTY DSDColumn.id STRING;
CREATE PROPERTY DSDColumn.title EMBEDDEDMAP STRING;
CREATE PROPERTY DSDColumn.supplemental EMBEDDEDMAP STRING;
CREATE PROPERTY DSDColumn.subject STRING;
CREATE PROPERTY DSDColumn.dataType STRING;
CREATE PROPERTY DSDColumn.values EMBEDDED DSDDomain;
CREATE PROPERTY DSDColumn.domain EMBEDDED DSDDomain;
CREATE PROPERTY DSDColumn.columnLink STRING;
CREATE PROPERTY DSDColumn.key BOOLEAN;
CREATE PROPERTY DSDColumn.transposed BOOLEAN;
CREATE PROPERTY DSDColumn.virtual BOOLEAN;

CREATE PROPERTY DSDDomain.codes EMBEDDEDLIST OjCodeList;
CREATE PROPERTY DSDDomain.enumeration EMBEDDEDLIST STRING;
CREATE PROPERTY DSDDomain.period EMBEDDED Period;
CREATE PROPERTY DSDDomain.timeList EMBEDDEDLIST LONG;





CREATE PROPERTY Code.codeList LINK MeIdentification;
CREATE PROPERTY Code.code STRING;
CREATE PROPERTY Code.level INTEGER;
CREATE PROPERTY Code.title EMBEDDEDMAP STRING;
CREATE PROPERTY Code.description EMBEDDEDMAP STRING;
CREATE PROPERTY Code.validityPeriod EMBEDDED Period;
CREATE PROPERTY Code.parents LINKLIST Code;
CREATE PROPERTY Code.children LINKLIST Code;
CREATE PROPERTY Code.relations LINKLIST Code;
CREATE PROPERTY Code.leaf boolean;
CREATE PROPERTY Code.indexLabel string;






CREATE PROPERTY MeIdentification.uid STRING;
CREATE PROPERTY MeIdentification.version STRING;
CREATE PROPERTY MeIdentification.parentIdentifiers EMBEDDEDLIST STRING;
CREATE PROPERTY MeIdentification.language EMBEDDED OjCodeList;
CREATE PROPERTY MeIdentification.languageDetails EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.title EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.creationDate DATETIME;
CREATE PROPERTY MeIdentification.characterSet EMBEDDED OjCodeList;
CREATE PROPERTY MeIdentification.metadataStandardName STRING;
CREATE PROPERTY MeIdentification.metadataStandardVersion STRING;
CREATE PROPERTY MeIdentification.metadataLanguage EMBEDDED OjCodeList;
CREATE PROPERTY MeIdentification.contacts EMBEDDEDLIST OjResponsibleParty;
CREATE PROPERTY MeIdentification.noDataValue STRING;

CREATE PROPERTY MeIdentification.meDocuments EMBEDDEDLIST MeDocuments;
  CREATE PROPERTY MeDocuments.document EMBEDDED OjCitation;
  CREATE PROPERTY MeDocuments.referenceEntity STRING;
  CREATE PROPERTY MeDocuments.referenceElement STRING;
CREATE PROPERTY MeIdentification.meInstitutionalMandate EMBEDDED MeInstitutionalMandate;
  CREATE PROPERTY MeInstitutionalMandate.legalActsAgreements EMBEDDEDMAP STRING;
  CREATE PROPERTY MeInstitutionalMandate.institutionalMandateDataSharing EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.meAccessibility EMBEDDED MeAccessibility;
  CREATE PROPERTY MeAccessibility.seDataDissemination EMBEDDED SeDataDissemination;
    CREATE PROPERTY SeDataDissemination.seDistribution EMBEDDED SeDistribution;
      CREATE PROPERTY SeDistribution.onlineResource STRING;
      CREATE PROPERTY SeDistribution.disseminationFormat EMBEDDEDLIST STRING;
    CREATE PROPERTY SeDataDissemination.seReleasePolicy EMBEDDED SeReleasePolicy;
      CREATE PROPERTY SeReleasePolicy.releaseCalendar EMBEDDEDMAP STRING;
      CREATE PROPERTY SeReleasePolicy.releaseCalendarAccess STRING;
      CREATE PROPERTY SeReleasePolicy.disseminationPeriodicity EMBEDDED OjCodeList;
      CREATE PROPERTY SeReleasePolicy.embargoTime EMBEDDED OjPeriod;
  CREATE PROPERTY MeAccessibility.seClarity EMBEDDED SeClarity;
    CREATE PROPERTY SeClarity.clarity EMBEDDEDMAP STRING;
    CREATE PROPERTY SeClarity.metadataCompletenessRate INTEGER;
  CREATE PROPERTY MeAccessibility.seConfidentiality EMBEDDED SeConfidentiality;
    CREATE PROPERTY SeConfidentiality.confidentialityPolicy EMBEDDEDMAP STRING;
    CREATE PROPERTY SeConfidentiality.confidentialityDataTreatment EMBEDDEDMAP STRING;
    CREATE PROPERTY SeConfidentiality.confidentialityStatus EMBEDDED OjCodeList;
CREATE PROPERTY MeIdentification.meContent EMBEDDED MeContent;
  CREATE PROPERTY MeContent.resourceRepresentationType STRING;
  CREATE PROPERTY MeContent.keywords EMBEDDEDLIST STRING;
  CREATE PROPERTY MeContent.description EMBEDDEDMAP STRING;
  CREATE PROPERTY MeContent.statisticalConceptsDefinitions EMBEDDEDMAP STRING;
  CREATE PROPERTY MeContent.seReferencePopulation EMBEDDED SeReferencePopulation;
    CREATE PROPERTY SeReferencePopulation.statisticalPopulation EMBEDDEDMAP STRING;
    CREATE PROPERTY SeReferencePopulation.statisticalUnit EMBEDDEDMAP STRING;
    CREATE PROPERTY SeReferencePopulation.referencePeriod EMBEDDED OjCodeList;
    CREATE PROPERTY SeReferencePopulation.referenceArea EMBEDDED OjCodeList;
  CREATE PROPERTY MeContent.seCoverage EMBEDDED SeCoverage;
    CREATE PROPERTY SeCoverage.coverageSectors EMBEDDED OjCodeList;
    CREATE PROPERTY SeCoverage.coverageSectorsDetails EMBEDDEDMAP STRING;
    CREATE PROPERTY SeCoverage.coverageTime EMBEDDED OjPeriod;
    CREATE PROPERTY SeCoverage.coverageGeographic EMBEDDED OjCodeList;
  CREATE PROPERTY MeContent.seCodeList EMBEDDED SeCodeList;
    CREATE PROPERTY SeCodeList.numberOfLevels INTEGER;
    CREATE PROPERTY SeCodeList.typeOfCodeList STRING;
CREATE PROPERTY MeIdentification.meDataQuality EMBEDDED MeDataQuality;
  CREATE PROPERTY MeDataQuality.qualityManagement EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.qualityAssessment EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.qualityAssurance EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.seAccuracy EMBEDDED SeAccuracy;
    CREATE PROPERTY SeAccuracy.accuracyNonSampling EMBEDDEDMAP STRING;
    CREATE PROPERTY SeAccuracy.accuracySampling EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.seDataRevision EMBEDDED SeDataRevision;
    CREATE PROPERTY SeDataRevision.revisionPolicy EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataRevision.revisionPractice EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.seRelevance EMBEDDED SeRelevance;
    CREATE PROPERTY SeRelevance.userNeeds EMBEDDEDMAP STRING;
    CREATE PROPERTY SeRelevance.userSatisfaction EMBEDDEDMAP STRING;
    CREATE PROPERTY SeRelevance.completeness EMBEDDEDMAP STRING;
    CREATE PROPERTY SeRelevance.completenessPercentage DOUBLE;
  CREATE PROPERTY MeDataQuality.seComparability EMBEDDED SeComparability;
    CREATE PROPERTY SeComparability.comparabilityGeographical EMBEDDEDMAP STRING;
    CREATE PROPERTY SeComparability.comparabilityTime EMBEDDEDMAP STRING;
    CREATE PROPERTY SeComparability.coherenceIntern EMBEDDEDMAP STRING;
  CREATE PROPERTY MeDataQuality.seTimelinessPunctuality EMBEDDED SeTimelinessPunctuality;
    CREATE PROPERTY SeTimelinessPunctuality.timeliness EMBEDDEDMAP STRING;
    CREATE PROPERTY SeTimelinessPunctuality.punctuality EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.meMaintenance EMBEDDED MeMaintenance;
  CREATE PROPERTY MeMaintenance.maintenanceAgency EMBEDDEDMAP STRING;
  CREATE PROPERTY MeMaintenance.seUpdate EMBEDDED SeUpdate;
    CREATE PROPERTY SeUpdate.updateDate DATETIME;
    CREATE PROPERTY SeUpdate.updatePeriodicity EMBEDDED OjCodeList;
  CREATE PROPERTY MeMaintenance.seMetadataMaintenance EMBEDDED SeMetadataMaintenance;
    CREATE PROPERTY SeMetadataMaintenance.metadataLastCertified DATE;
    CREATE PROPERTY SeMetadataMaintenance.metadataLastPosted DATE;
    CREATE PROPERTY SeMetadataMaintenance.metadataLastUpdate DATE;
CREATE PROPERTY MeIdentification.meResourceStructure EMBEDDED MeResourceStructure;
  CREATE PROPERTY MeResourceStructure.seResourceDimensions EMBEDDEDLIST SeResourceDimensions;
    CREATE PROPERTY SeResourceDimensions.dimensionName EMBEDDEDMAP STRING;
    CREATE PROPERTY SeResourceDimensions.dimensionType STRING;
    CREATE PROPERTY SeResourceDimensions.dimensionSubject EMBEDDED OjCodeList;
    CREATE PROPERTY SeResourceDimensions.dimensionContent EMBEDDED;
    CREATE PROPERTY SeResourceDimensions.dimensionMU EMBEDDED OjMeasure;
  CREATE PROPERTY MeResourceStructure.seResourceRecords EMBEDDEDLIST SeResourceRecords;
    CREATE PROPERTY SeResourceRecords.recordID STRING;
    CREATE PROPERTY SeResourceRecords.originOfCollectedValue EMBEDDED OjCodeList;
    CREATE PROPERTY SeResourceRecords.creationDate DATE;
    CREATE PROPERTY SeResourceRecords.dataReliabilityQualifier EMBEDDEDMAP STRING;
    CREATE PROPERTY SeResourceRecords.dataReliabilityIndicator DOUBLE;
    CREATE PROPERTY SeResourceRecords.confidentialityStatus STRING;
    CREATE PROPERTY SeResourceRecords.observationStatus EMBEDDED OjCodeList;
    CREATE PROPERTY SeResourceRecords.remarks EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.meReferenceSystem EMBEDDED MeReferenceSystem;
  CREATE PROPERTY MeReferenceSystem.referenceSystemIdentifier STRING;
  CREATE PROPERTY MeReferenceSystem.referenceSystemName EMBEDDEDMAP STRING;
  CREATE PROPERTY MeReferenceSystem.referenceSystemAuthority EMBEDDED OjResponsibleParty;
  CREATE PROPERTY MeReferenceSystem.seProjection EMBEDDED SeProjection;
    CREATE PROPERTY SeProjection.projection EMBEDDED OjCodeList;
    CREATE PROPERTY SeProjection.projectionName EMBEDDEDMAP STRING;
    CREATE PROPERTY SeProjection.seProjectionParameters EMBEDDED SeProjectionParameters;
      CREATE PROPERTY SeProjectionParameters.zone INTEGER;
      CREATE PROPERTY SeProjectionParameters.standardParallel DOUBLE;
      CREATE PROPERTY SeProjectionParameters.longitudeOfCentralMeridian DOUBLE;
      CREATE PROPERTY SeProjectionParameters.latitudeOfProjectionOrigin DOUBLE;
      CREATE PROPERTY SeProjectionParameters.falseEasting DOUBLE;
      CREATE PROPERTY SeProjectionParameters.falseNorthing DOUBLE;
      CREATE PROPERTY SeProjectionParameters.falseEastingNorthingUnits EMBEDDED OjMeasure;
      CREATE PROPERTY SeProjectionParameters.scaleFactorAtEquator DOUBLE;
      CREATE PROPERTY SeProjectionParameters.heightOfProspectivePointAboveSurface DOUBLE;
      CREATE PROPERTY SeProjectionParameters.longitudeOfProjectionCenter DOUBLE;
      CREATE PROPERTY SeProjectionParameters.latitudeOfProjectionCenter DOUBLE;
      CREATE PROPERTY SeProjectionParameters.scaleFactorAtCenterLine DOUBLE;
      CREATE PROPERTY SeProjectionParameters.straightVerticalLongitudeFromPole DOUBLE;
      CREATE PROPERTY SeProjectionParameters.scaleFactorAtProjectionOrigin DOUBLE;
      CREATE PROPERTY SeProjectionParameters.seObliqueLineAzimuth EMBEDDED SeObliqueLineAzimuth;
        CREATE PROPERTY SeObliqueLineAzimuth.azimuthAngle DOUBLE;
        CREATE PROPERTY SeObliqueLineAzimuth.azimuthMeasurePointLongitude DOUBLE;
      CREATE PROPERTY SeProjectionParameters.seObliqueLinePoint EMBEDDED SeObliqueLinePoint;
        CREATE PROPERTY SeObliqueLinePoint.obliqueLineLatitude DOUBLE;
        CREATE PROPERTY SeObliqueLinePoint.obliqueLineLongitude DOUBLE;
  CREATE PROPERTY MeReferenceSystem.seEllipsoid EMBEDDED SeEllipsoid;
    CREATE PROPERTY SeEllipsoid.ellipsoid EMBEDDED OjCodeList;
    CREATE PROPERTY SeEllipsoid.ellipsoidName EMBEDDEDMAP STRING;
    CREATE PROPERTY SeEllipsoid.seEllipsoidParameters EMBEDDED SeEllipsoidParameters;
      CREATE PROPERTY SeEllipsoidParameters.semiMajorAxis DOUBLE;
      CREATE PROPERTY SeEllipsoidParameters.axisUnits EMBEDDED OjMeasure;
      CREATE PROPERTY SeEllipsoidParameters.denominatorOfFlatteringRatio DOUBLE;
  CREATE PROPERTY MeReferenceSystem.seDatum EMBEDDED SeDatum;
    CREATE PROPERTY SeDatum.datum EMBEDDED OjCodeList;
    CREATE PROPERTY SeDatum.datumName EMBEDDEDMAP STRING;
CREATE PROPERTY MeIdentification.meSpatialRepresentation EMBEDDED MeSpatialRepresentation;
  CREATE PROPERTY MeSpatialRepresentation.layerType STRING;
  CREATE PROPERTY MeSpatialRepresentation.processing EMBEDDED OjCodeList;
  CREATE PROPERTY MeSpatialRepresentation.typeOfProduct EMBEDDED OjCodeList;
  CREATE PROPERTY MeSpatialRepresentation.noDataValue STRING;
  CREATE PROPERTY MeSpatialRepresentation.seBoundingBox EMBEDDED SeBoundingBox;
    CREATE PROPERTY SeBoundingBox.xmin DOUBLE;
    CREATE PROPERTY SeBoundingBox.xmax DOUBLE;
    CREATE PROPERTY SeBoundingBox.ymin DOUBLE;
    CREATE PROPERTY SeBoundingBox.ymax DOUBLE;
    CREATE PROPERTY SeBoundingBox.seGridSpatialRepresentation EMBEDDED SeGridSpatialRepresentation;
      CREATE PROPERTY SeGridSpatialRepresentation.numberOfDimensions INTEGER;
      CREATE PROPERTY SeGridSpatialRepresentation.axisDimensionProperties EMBEDDED OjAxis;
      CREATE PROPERTY SeGridSpatialRepresentation.cellGeometry STRING;
      CREATE PROPERTY SeGridSpatialRepresentation.cellOfOrigin STRING;
      CREATE PROPERTY SeGridSpatialRepresentation.xyPosition STRING;
    CREATE PROPERTY SeBoundingBox.seVectorSpatialRepresentation EMBEDDED SeVectorSpatialRepresentation;
      CREATE PROPERTY SeVectorSpatialRepresentation.topologyLevel EMBEDDED OjCodeList;
      CREATE PROPERTY SeVectorSpatialRepresentation.geometricObjects STRING;
CREATE PROPERTY MeIdentification.meStatisticalProcessing EMBEDDED MeStatisticalProcessing;
  CREATE PROPERTY MeStatisticalProcessing.seDataSource EMBEDDED SeDataSource;
    CREATE PROPERTY SeDataSource.sePrimaryDataCollection EMBEDDED SePrimaryDataCollection;
      CREATE PROPERTY SePrimaryDataCollection.dataCollector EMBEDDED OjResponsibleParty;
      CREATE PROPERTY SePrimaryDataCollection.typeOfCollection EMBEDDED OjCodeList;
      CREATE PROPERTY SePrimaryDataCollection.samplingProcedure EMBEDDEDMAP STRING;
      CREATE PROPERTY SePrimaryDataCollection.dataCollection EMBEDDEDMAP STRING;
      CREATE PROPERTY SePrimaryDataCollection.collectionPeriodicity EMBEDDED OjCodeList;
    CREATE PROPERTY SeDataSource.seSecondaryDataCollection EMBEDDED SeSecondaryDataCollection;
      CREATE PROPERTY SeSecondaryDataCollection.originOfCollectedData EMBEDDED OjCodeList;
      CREATE PROPERTY SeSecondaryDataCollection.organization EMBEDDEDMAP STRING;
      CREATE PROPERTY SeSecondaryDataCollection.rawDataDescription EMBEDDEDMAP STRING;
      CREATE PROPERTY SeSecondaryDataCollection.dataCollection EMBEDDEDMAP STRING;
  CREATE PROPERTY MeStatisticalProcessing.seDataCompilation EMBEDDED SeDataCompilation;
    CREATE PROPERTY SeDataCompilation.missingData EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataCompilation.weights EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataCompilation.aggregationProcessing EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataCompilation.aggregationFormula STRING;
    CREATE PROPERTY SeDataCompilation.dataAdjustment EMBEDDED OjCodeList;
    CREATE PROPERTY SeDataCompilation.dataAdjustmentDetails EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataCompilation.indexType EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataCompilation.basePeriod DATE;
  CREATE PROPERTY MeStatisticalProcessing.seDataValidation EMBEDDED SeDataValidation;
    CREATE PROPERTY SeDataValidation.dataValidationIntermediate EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataValidation.dataValidationOutput EMBEDDEDMAP STRING;
    CREATE PROPERTY SeDataValidation.dataValidationSource EMBEDDEDMAP STRING;

CREATE PROPERTY MeIdentification.dsd LINK DSD;







CREATE PROPERTY MeIdentification.index_id STRING;
CREATE PROPERTY MeIdentification.index_meContent|resourceRepresentationType STRING;

CREATE INDEX MeIdentification.index_id UNIQUE;
CREATE INDEX MeIdentification.index_meContent|resourceRepresentationType NOTUNIQUE;

CREATE INDEX Code.codesFilter ON Code (codeList, level, code) NOTUNIQUE;
CREATE INDEX Code.indexLabel FULLTEXT ENGINE LUCENE;

CREATE INDEX DSDGeographic.workspace|layerName ON DSDGeographic (workspace, layerName) UNIQUE;

CREATE INDEX MeIdentification.dsd NOTUNIQUE;

DISCONNECT;