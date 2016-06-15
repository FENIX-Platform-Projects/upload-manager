package org.fao.ess.uploader.oecd.policy.bulk;

import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.process.PostUpload;
import org.fao.ess.uploader.core.process.ProcessInfo;
import org.fao.ess.uploader.core.storage.BinaryStorage;
import org.fao.ess.uploader.oecd.policy.bulk.attachments.dto.AttachmentProperties;
import org.fao.ess.uploader.oecd.policy.bulk.data.impl.CodeList;
import org.fao.ess.uploader.oecd.policy.bulk.data.impl.CodeListManager;
import org.fao.ess.uploader.oecd.policy.bulk.data.impl.DataManager;
import org.fao.ess.uploader.oecd.policy.bulk.metadata.D3SManager;
import org.fao.ess.uploader.oecd.policy.bulk.attachments.impl.FileManager;
import org.fao.ess.uploader.oecd.policy.bulk.utils.D3SClient;
import org.fao.fenix.commons.msd.dto.data.Resource;
import org.fao.fenix.commons.msd.dto.full.Code;
import org.fao.fenix.commons.msd.dto.full.DSDCodelist;
import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;

import javax.inject.Inject;
import javax.ws.rs.BadRequestException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.*;
import java.util.regex.Pattern;

@ProcessInfo(context = "policy.bulk", name = "PolicyBulk", priority = 1)
public class PolicyBulk implements PostUpload {
    @Inject private D3SManager metadataManager;
    @Inject private FileManager tmpFileManager;
    @Inject private DataManager dataManager;
    @Inject private CodeListManager codeListManager;
    @Inject private D3SClient d3SClient;


    @Override
    public void chunkUploaded(ChunkMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage) throws Exception {
        //Nothing to do here
    }

    @Override
    public void fileUploaded(FileMetadata metadata, MetadataStorage metadataStorage, BinaryStorage storage, Map<String, Object> processingParams) throws Exception {
        String source = (String)processingParams.get("source");
        if (source==null)
            throw new BadRequestException("Source is undefined");
        InputStream zipFileInput = storage.readFile(metadata, null);
        //Start post processing
        mainLogic(source, zipFileInput);
    }

    public void mainLogic(String source, InputStream zipFileInput) throws Exception {
        //Create temporary folder with zip file content
        tmpFileManager.createTmpFolder(zipFileInput, source);
        try {
            //Read dataset metadata using 'dsdDefault' template for dsd
            Collection<MeIdentification<DSDDataset>> metadataList = metadataManager.createMetadata(tmpFileManager.getMetadataFileStream(source), "dsdDefault");
            Connection databaseConnection = dataManager.getConnection();
            try {
                //Retrieve existing metadataIds
                Collection<String> legacyMetadata = dataManager.getLegacyMetadataIds(source, databaseConnection);
                //Upload CSV data
                String backupTableName = dataManager.createBackupTable(source, databaseConnection);
                dataManager.uploadCSV(tmpFileManager.getDataFileStream(source), backupTableName, databaseConnection);
                //Manage negative sub-national codes (update correspondent metadata uid)
                updateMetadataUid(
                        metadataList,
                        dataManager.getSubnationalCodesTranscodeMap(backupTableName, databaseConnection)
                );
                //Create and verify metadata id
                checkMetadataId(
                        metadataList,
                        dataManager.createMetadataId(backupTableName, databaseConnection)
                );
                //Remove existing source related data from the original database
                dataManager.removeStrictlySourceRelatedData(source, backupTableName, databaseConnection);
                //Create new policies id
                Integer[] policiesId = dataManager.createPolicyId(backupTableName, databaseConnection);

                //Upload attachments into the repository folder
                Collection<AttachmentProperties> attachmentsMetadata = tmpFileManager.uploadAttachments(source, policiesId);
                try {
                    //Insert attachments metadata into the database
                    dataManager.updateAttachmentsData(attachmentsMetadata, backupTableName, databaseConnection);

                    //publish temporary data into the database
                    dataManager.finishDataPublication(source, backupTableName, databaseConnection);

                    //Insert or update datasets metadata
                    metadataManager.deleteLegacyMetadata(legacyMetadata);
                    metadataManager.sendMetadata(metadataList);
                    //Update codelists into D3S
                    metadataManager.sendCodelists(getCodelists(databaseConnection));
                    //Update update date of related views into D3S
                    metadataManager.sendLastUpdateDateUpdate("oecd_view");

                    //Commit database changes
                    databaseConnection.commit();
                } catch (Exception ex) {
                    tmpFileManager.restoreAttachments(source);
                    throw ex;
                }
            } catch (Exception ex) {
                databaseConnection.rollback();
                throw ex;
            } finally {
                databaseConnection.close();
            }
        } finally {
            //Remove tmp folder
            tmpFileManager.removeTmpFolder(source);
        }
    }

    private Collection<Resource<DSDCodelist, Code>> getCodelists (Connection databaseConnection) throws Exception {
        Collection<Resource<DSDCodelist, Code>> resources = new LinkedList<>();
        for (CodeList codeList : CodeList.values())
            resources.add(codeListManager.getSimpleCodeList(databaseConnection, codeList));
        return resources;
    }



    //Utils
    private void updateMetadataUid (Collection<MeIdentification<DSDDataset>> metadataList, Map<Integer, Integer> subnationalCodesTranscodeMap) throws Exception {
        for (Map.Entry<Integer, Integer> transCodeEntry : subnationalCodesTranscodeMap.entrySet()) {
            Pattern pattern = Pattern.compile("^POLICY_\\d+_"+transCodeEntry.getKey()+"_*$");
            for (MeIdentification<DSDDataset> metadata : metadataList)
                if (pattern.matcher(metadata.getUid()).matches()) {
                    metadata.setUid(metadata.getUid().replace(transCodeEntry.getKey().toString(), transCodeEntry.getValue().toString()));
                    return;
                }
            throw new BadRequestException("No metadata found for sub-national code "+transCodeEntry.getKey());
        }
    }

    private void checkMetadataId(Collection<MeIdentification<DSDDataset>> metadataList, Collection<String> metadataIdList) throws Exception {
        Set<String> metadataIdSet = new HashSet<>(metadataIdList);
        Set<String> metadataUidSet = new HashSet<>();
        for (MeIdentification<DSDDataset> metadata : metadataList)
            metadataUidSet.add(metadata.getUid());

        if (metadataUidSet.size()!=metadataList.size())
            throw new BadRequestException("Metadata id duplication found into metadata Excel file");

        metadataIdSet.removeAll(metadataUidSet);
        metadataUidSet.removeAll(metadataIdList);
        if (metadataIdSet.size()>0 || metadataUidSet.size()>0) {
            StringBuilder error = new StringBuilder("Metadata ID miss matching between data and metadata.");
            if (metadataIdSet.size()>0) {
                error.append("\n\nData file refer to a set of metadata id with no match into the metadata file:");
                for (String id : metadataIdSet)
                    error.append('\n').append(id);
            }
            if (metadataUidSet.size()>0) {
                error.append("\n\nMetadata file contains a set of metadata id with no match into the data file:");
                for (String id : metadataUidSet)
                    error.append('\n').append(id);
            }
            throw new BadRequestException(error.toString());
        }
    }

}