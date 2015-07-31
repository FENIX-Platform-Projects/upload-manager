package org.fao.ess.uploader.core.metadata.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;
import org.fao.ess.uploader.core.dto.ProcessMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.metadata.MetadataStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.NoContentException;
import java.util.*;

@ApplicationScoped
public class OrientMetadataStorage extends MetadataStorage {
    @Inject private UploaderConfig config;
    @Inject private OrientClient ds;

    @Override
    public void init() throws Exception {
        ds.initPool(config.get("metadata.url"),config.get("metadata.usr"),config.get("metadata.psw"));
    }

    @Override
    public FileMetadata load(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return toFileMetadata(loadDoc(connection, context, md5));
        } finally {
            connection.close();
        }
    }
    public ODocument loadDoc(ODatabaseDocumentTx connection, String context, String md5) throws Exception {
        List<ODocument> metadataList = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM FileMetadata WHERE context = ? AND md5 = ?"), context, md5);
        return metadataList.size()==1 ? metadataList.iterator().next() : null;
    }

    @Override
    public ChunkMetadata load(String context, String md5, Integer index) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return toChunkMetadata(loadDoc(connection, context, md5, index));
        } finally {
            connection.close();
        }
    }
    public ODocument loadDoc(ODatabaseDocumentTx connection, String context, String md5, Integer index) throws Exception {
        ODocument fileMetadataDoc = loadDoc(connection, context, md5);
        return fileMetadataDoc!=null ? loadChunkDoc(connection, fileMetadataDoc, index) : null;
    }

    @Override
    public Collection<ChunkMetadata> loadChunks(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return toChunkMetadata(loadChunksDoc(connection, context, md5));
        } finally {
            connection.close();
        }
    }
    public Collection<ODocument> loadChunksDoc(ODatabaseDocumentTx connection, String context, String md5) throws Exception {
        ODocument fileMetadataDoc = loadDoc(connection, context, md5);
        return (List<ODocument>)connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ChunkMetadata WHERE file = ?"), fileMetadataDoc.getIdentity());
    }
    public ODocument loadChunkDoc(ODatabaseDocumentTx connection, ODocument fileMetadataDoc, int index) throws Exception {
        List<ODocument> chunks = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ChunkMetadata WHERE file = ? and index = ?"), fileMetadataDoc.getIdentity(), index);
        return chunks.size()==1 ? chunks.iterator().next() : null;
    }

    @Override
    public Collection<FileMetadata> select(String context) throws Exception {
        return toFileMetadata(selectDoc(context));
    }

    @Override
    public Collection<ProcessMetadata> loadProcesses(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return toProcessMetadata(loadProcessesDoc(connection, context, md5));
        } finally {
            connection.close();
        }
    }
    public Collection<ODocument> loadProcessesDoc(ODatabaseDocumentTx connection, String context, String md5) throws Exception {
        ODocument fileMetadataDoc = loadDoc(connection, context, md5);
        return (List<ODocument>)connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ProcessMetadata WHERE file = ? ORDER BY index"), fileMetadataDoc.getIdentity());
    }
    public ODocument loadProcessDoc(ODatabaseDocumentTx connection, ODocument fileMetadataDoc, int index) throws Exception {
        List<ODocument> processes = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ProcessMetadata WHERE file = ? and index = ?"), fileMetadataDoc.getIdentity(), index);
        return processes.size()==1 ? processes.iterator().next() : null;
    }

    public Collection<ODocument> selectDoc(String context) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return (List<ODocument>)connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM FileMetadata WHERE context = ?"), context);
        } finally {
            connection.close();
        }
    }

    @Override
    public void save(FileMetadata metadata) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.save(toDocument(metadata, loadDoc(connection, metadata.getContext(), metadata.getMd5()), false));
        } finally {
            connection.close();
        }
    }

    @Override
    public void save(ChunkMetadata metadata) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            FileMetadata fileMetadata = metadata.getFile();
            ODocument fileMetadataDocument = fileMetadata!=null ? loadDoc(connection, fileMetadata.getContext(), fileMetadata.getMd5()) : null;
            if (fileMetadataDocument==null)
                throw new NoContentException("context: "+(fileMetadata!=null?fileMetadata.getContext():null)+" - md5: "+(fileMetadata!=null?fileMetadata.getMd5():null));

            connection.save(toDocument(metadata, fileMetadataDocument, loadChunkDoc(connection, fileMetadataDocument, metadata.getIndex()), false));
        } finally {
            connection.close();
        }
    }

    @Override
    public void save(ProcessMetadata metadata) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            FileMetadata fileMetadata = metadata.getFile();
            ODocument fileMetadataDocument = fileMetadata!=null ? loadDoc(connection, fileMetadata.getContext(), fileMetadata.getMd5()) : null;
            if (fileMetadataDocument==null)
                throw new NoContentException("context: "+(fileMetadata!=null?fileMetadata.getContext():null)+" - md5: "+(fileMetadata!=null?fileMetadata.getMd5():null));

            connection.save(toDocument(metadata, fileMetadataDocument, loadProcessDoc(connection, fileMetadataDocument, metadata.getIndex()), false));
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean remove(String context,String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.begin();
            ODocument fileDoc = loadDoc(connection, context, md5);
            if (fileDoc==null)
                return false;
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file = ?")).execute(fileDoc.getIdentity());
            connection.command(new OCommandSQL("DELETE FROM ProcessMetadata WHERE file = ?")).execute(fileDoc.getIdentity());
            connection.command(new OCommandSQL("DELETE FROM FileMetadata WHERE context = ? AND md5 = ? ")).execute(context, md5);
            connection.commit();
            return true;
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean removeProcess(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            ODocument fileDoc = loadDoc(connection, context, md5);
            if (fileDoc==null)
                return false;
            int n = connection.command(new OCommandSQL("DELETE FROM ProcessMetadata WHERE file = ?")).execute(fileDoc.getIdentity());
            return n>0;
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean remove(String context, String md5, int index) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            ODocument fileDoc = loadDoc(connection, context, md5);
            if (fileDoc==null)
                return false;
            int n = connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file = ? and index = ?")).execute(fileDoc.getIdentity(), index);
            return n>0;
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean removeAll(String context) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.begin();
            List<ODocument> fileMetadataList = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM FileMetadata WHERE context = ?"), context);
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( ? )")).execute(fileMetadataList);
            connection.command(new OCommandSQL("DELETE FROM ProcessMetadata WHERE file IN ( ? )")).execute(fileMetadataList);
            int n = connection.command(new OCommandSQL("DELETE FROM FileMetadata WHERE context = ?")).execute(context);
            connection.commit();
            return n>0;
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.close();
        }
    }



    //Utils
    private FileMetadata toFileMetadata(ODocument document) throws Exception {
        if (document!=null) {
            FileMetadata metadata = new FileMetadata();
            metadata.setContext((String) document.field("context"));
            metadata.setMd5((String) document.field("md5"));
            metadata.setName((String) document.field("name"));
            metadata.setDate((Date) document.field("date"));
            metadata.setSize((Long) document.field("size"));
            metadata.setZip((Boolean) document.field("zip"));
            metadata.setChunksNumber((Integer) document.field("chunksNumber"));
            metadata.setAutoClose((Boolean) document.field("autoClose"));
            metadata.setProperties((Map<String, Object>) document.field("properties"));
            metadata.setStatus(toFileStatus((ODocument) document.field("status")));
            return metadata;
        } else
            return null;
    }
    private Collection<FileMetadata> toFileMetadata(Collection<ODocument> documents) throws Exception {
        if (documents!=null) {
            Collection<FileMetadata> metadataList = new LinkedList<>();
            for (ODocument document : documents)
                metadataList.add(toFileMetadata(document));
            return metadataList;
        } else
            return null;
    }
    private FileStatus toFileStatus (ODocument document) throws Exception {
        if (document!=null) {
            FileStatus status = new FileStatus();
            status.setChunksIndex((Set<Integer>) document.field("chunksIndex"));
            status.setCurrentSize((Long) document.field("currentSize"));
            status.setComplete((Boolean) document.field("complete"));
            status.setError((String) document.field("error"));
            return status;
        } else
            return null;
    }

    private ChunkMetadata toChunkMetadata(ODocument document) throws Exception {
        if (document!=null) {
            ChunkMetadata metadata = new ChunkMetadata();
            metadata.setFile(toFileMetadata((ODocument) document.field("file")));
            metadata.setSize((Long) document.field("size"));
            metadata.setIndex((Integer) document.field("index"));
            metadata.setUploaded(document.field("uploaded")!=null ? (Boolean) document.field("uploaded") : false);
            return metadata;
        } else
            return null;
    }
    private Collection<ChunkMetadata> toChunkMetadata(Collection<ODocument> documents) throws Exception {
        if (documents!=null) {
            Collection<ChunkMetadata> metadataList = new LinkedList<>();
            for (ODocument document : documents)
                metadataList.add(toChunkMetadata(document));
            return metadataList;
        } else
            return null;
    }

    private ProcessMetadata toProcessMetadata(ODocument document) throws Exception {
        if (document!=null) {
            ProcessMetadata metadata = new ProcessMetadata();
            metadata.setFile(toFileMetadata((ODocument) document.field("file")));
            metadata.setName((String) document.field("name"));
            metadata.setError((String) document.field("error"));
            metadata.setIndex((Integer) document.field("index"));
            metadata.setCompleted((Boolean) document.field("completed"));
            return metadata;
        } else
            return null;
    }
    private Collection<ProcessMetadata> toProcessMetadata(Collection<ODocument> documents) throws Exception {
        if (documents!=null) {
            Collection<ProcessMetadata> metadataList = new LinkedList<>();
            for (ODocument document : documents)
                metadataList.add(toProcessMetadata(document));
            return metadataList;
        } else
            return null;
    }

    private ODocument toDocument (FileMetadata metadata, ODocument document, boolean overwrite) throws Exception {
        ODocument statusDoc = toDocument(metadata.getStatus(), document!=null ? (ODocument) document.field("status") : null, overwrite);
        return toDocument(
                FileMetadata.class.getSimpleName(),
                new String[]{"context","md5","name","date","size","zip","chunksNumber","properties","autoClose","status"},
                new Object[]{metadata.getContext(), metadata.getMd5(), metadata.getName(), metadata.getDate(), metadata.getSize(), metadata.isZip(), metadata.getChunksNumber(), metadata.getProperties(), metadata.isAutoClose(), statusDoc },
                new OType[][]{{OType.STRING},{OType.STRING},{OType.STRING},{OType.DATETIME},{OType.LONG},{OType.BOOLEAN},{OType.INTEGER},{OType.EMBEDDEDMAP},{OType.BOOLEAN},{OType.EMBEDDED}},
                document, overwrite
        );
    }
    private ODocument toDocument (FileStatus status, ODocument document, boolean overwrite) throws Exception {
        return toDocument(
                FileStatus.class.getSimpleName(),
                new String[]{"currentSize","chunksIndex","complete","error"},
                new Object[]{status.getCurrentSize(), status.getChunksIndex(), status.getComplete(), status.getError() },
                new OType[][]{{OType.LONG},{OType.EMBEDDEDSET, OType.INTEGER},{OType.BOOLEAN},{OType.STRING}},
                document, overwrite
        );
    }
    private ODocument toDocument (ChunkMetadata metadata, ODocument fileMetadataDoc, ODocument document, boolean overwrite) throws Exception {
        return toDocument(
                ChunkMetadata.class.getSimpleName(),
                new String[]{"file","index","size","uploaded"},
                new Object[]{fileMetadataDoc, metadata.getIndex(), metadata.getSize(), metadata.isUploaded()},
                new OType[][]{{OType.LINK},{OType.INTEGER},{OType.LONG},{OType.BOOLEAN}},
                document, overwrite
        );
    }
    private ODocument toDocument (ProcessMetadata metadata, ODocument fileMetadataDoc, ODocument document, boolean overwrite) throws Exception {
        return toDocument(
                ProcessMetadata.class.getSimpleName(),
                new String[]{"file","index","name","error","completed"},
                new Object[]{fileMetadataDoc, metadata.getIndex(), metadata.getName(), metadata.getError(), metadata.isCompleted()},
                new OType[][]{{OType.LINK},{OType.INTEGER},{OType.STRING},{OType.STRING},{OType.BOOLEAN}},
                document, overwrite
        );
    }

    private ODocument toDocument(String className, String[] names, Object[] values, OType[][] types, ODocument document, boolean overwrite) throws Exception {
        if (document==null)
            document = new ODocument(className);

        for (int i=0; i<names.length; i++)
            if (values[i]!=null || overwrite)
                document.field(names[i],values[i],types[i]);

        return document;
    }
}
