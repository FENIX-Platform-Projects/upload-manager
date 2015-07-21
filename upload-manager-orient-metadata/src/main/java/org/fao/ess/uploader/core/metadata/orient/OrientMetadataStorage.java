package org.fao.ess.uploader.core.metadata.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.fao.ess.uploader.core.dto.ChunkMetadata;
import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.init.UploaderConfig;
import org.fao.ess.uploader.core.metadata.MetadataStorage;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.NoContentException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

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
    public void remove(String context,String md5) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.begin();
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? AND md5 = ? )")).execute(context, md5);
            int n = connection.command(new OCommandSQL("DELETE FROM FileMetadata WHERE context = ? AND md5 = ? ")).execute(context, md5);
            if (n == 0)
                throw new NoContentException("context: " + context + " - md5: " + md5);
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.close();
        }
    }

    @Override
    public void remove(String context, String md5, int index) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            int n = connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? AND md5 = ? ) and index = ?")).execute(context, md5, index);
            if (n == 0)
                throw new NoContentException("context: " + context + " - md5: " + md5);
        } finally {
            connection.close();
        }
    }

    @Override
    public void removeAll(String context) throws Exception {
        ODatabaseDocumentTx connection = ds.getConnection();
        try {
            connection.begin();
            connection.command(new OCommandSQL("DELETE FROM ChunkMetadata WHERE file IN ( SELECT FROM FileMetadata WHERE context = ? )")).execute(context);
            connection.command(new OCommandSQL("DELETE FROM FileMetadata WHERE context = ?")).execute(context);
            connection.commit();
        } catch (Exception ex) {
            connection.rollback();
            throw ex;
        } finally {
            connection.close();
        }
    }



    //Utils
    private FileMetadata toFileMetadata(ODocument document) throws Exception {
        return null;
    }
    private Collection<FileMetadata> toFileMetadata(Collection<ODocument> document) throws Exception {
        return null;
    }
    private ChunkMetadata toChunkMetadata(ODocument document) throws Exception {
        return null;
    }
    private Collection<ChunkMetadata> toChunkMetadata(Collection<ODocument> document) throws Exception {
        return null;
    }
    private ODocument toDocument (FileMetadata metadata, ODocument document, boolean overwrite) throws Exception {
        return null;
    }
    private ODocument toDocument (ChunkMetadata metadata, ODocument fileMetadataDoc, ODocument document, boolean overwrite) throws Exception {
        return null;
    }
}
