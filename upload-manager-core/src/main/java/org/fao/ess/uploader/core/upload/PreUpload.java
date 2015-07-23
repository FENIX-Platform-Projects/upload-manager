package org.fao.ess.uploader.core.upload;

import org.fao.ess.uploader.core.dto.ChunkMetadata;

import java.io.InputStream;

public interface PreUpload {

    InputStream uploadingChunk (ChunkMetadata metadata, InputStream stream) throws Exception;

}
