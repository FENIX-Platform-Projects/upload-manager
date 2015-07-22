package org.fao.ess.uploader.core.metadata.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.FileStatus;
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
        return toFileMetadata(loadDoc(context, md5));
    }
    public ODocument loadDoc(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            List<ODocument> metadataList = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM FileMetadata WHERE context = ? AND md5 = ?"), context, md5);
            return metadataList.size()==1 ? metadataList.iterator().next() : null;
        } finally {
            connection.close();
        }
    }

    @Override
    public Collection<ChunkMetadata> loadChunks(String context, String md5) throws Exception {
        return toChunkMetadata(loadChunksDoc(context,md5));
    }
    public Collection<ODocument> loadChunksDoc(String context, String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? AND md5 = ? )"), context, md5);
        } finally {
            connection.close();
        }
    }
    public ODocument loadChunkDoc(ODocument fileMetadataDoc, int index) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            List<ODocument> chunks = connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM ChunkMetadata WHERE file = ? and index = ?"), fileMetadataDoc, index);
            return chunks.size()==1 ? chunks.iterator().next() : null;
        } finally {
            connection.close();
        }
    }

    @Override
    public Collection<FileMetadata> select(String context) throws Exception {
        return toFileMetadata(selectDoc(context));
    }
    public Collection<ODocument> selectDoc(String context) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            return connection.query(new OSQLSynchQuery<ODocument>("SELECT FROM FileMetadata WHERE context = ?"), context);
        } finally {
            connection.close();
        }
    }

    @Override
    public void save(FileMetadata metadata) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.save(toDocument(metadata, loadDoc(metadata.getContext(), metadata.getMd5()), false));
        } finally {
            connection.close();
        }
    }

    @Override
    public void save(ChunkMetadata metadata) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            FileMetadata fileMetadata = metadata.getFile();
            ODocument fileMetadataDocument = fileMetadata!=null ? loadDoc(fileMetadata.getContext(), fileMetadata.getMd5()) : null;
            if (fileMetadataDocument==null)
                throw new NoContentException("context: "+(fileMetadata!=null?fileMetadata.getContext():null)+" - md5: "+(fileMetadata!=null?fileMetadata.getMd5():null));

            connection.save(toDocument(metadata, fileMetadataDocument, loadChunkDoc(fileMetadataDocument, metadata.getIndex()), false));
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean remove(String context,String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.begin();
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? AND md5 = ? )")).execute(context, md5);
            int n = connection.command(new OCommandSQL("DELETE FROM FileMetadata WHERE context = ? AND md5 = ? ")).execute(context, md5);
            connection.commit();
            return n>0;
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.close();
        }
    }

    @Override
    public boolean remove(String context, String md5, int index) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            int n = connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? AND md5 = ? ) and index = ?")).execute(context, md5, index);
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
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? )")).execute(context);
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

    private ODocument toDocument (FileMetadata metadata, ODocument document, boolean overwrite) throws Exception {
        ODocument statusDoc = toDocument(metadata.getStatus(), document!=null ? (ODocument) document.field("status") : null, overwrite);
        return toDocument(
                FileMetadata.class.getName(),
                new String[]{"context","md5","name","date","size","zip","chunksNumber","properties","status"},
                new Object[]{metadata.getContext(), metadata.getMd5(), metadata.getName(), metadata.getDate(), metadata.getSize(), metadata.isZip(), metadata.getChunksNumber(), metadata.getProperties(), statusDoc },
                new OType[][]{{OType.STRING},{OType.STRING},{OType.STRING},{OType.DATETIME},{OType.LONG},{OType.BOOLEAN},{OType.INTEGER},{OType.EMBEDDEDMAP},{OType.EMBEDDED}},
                document, overwrite
        );
    }
    private ODocument toDocument (FileStatus status, ODocument document, boolean overwrite) throws Exception {
        return toDocument(
                FileStatus.class.getName(),
                new String[]{"currentSize","chunksIndex","complete"},
                new Object[]{status.getCurrentSize(), status.getChunksIndex(), status.isComplete() },
                new OType[][]{{OType.LONG},{OType.EMBEDDEDSET, OType.INTEGER},{OType.BOOLEAN}},
                document, overwrite
        );
    }
    private ODocument toDocument (ChunkMetadata metadata, ODocument fileMetadataDoc, ODocument document, boolean overwrite) throws Exception {
        return toDocument(
                FileStatus.class.getName(),
                new String[]{"file","index","size"},
                new Object[]{fileMetadataDoc, metadata.getIndex(), metadata.getSize()},
                new OType[][]{{OType.LINK},{OType.INTEGER},{OType.LONG}},
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
