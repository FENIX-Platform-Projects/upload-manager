CONNECT remote:localhost/uploader_metadata admin admin;



CREATE CLASS FileStatus;
CREATE CLASS FileMetadata;
CREATE CLASS ChunkMetadata;



CREATE PROPERTY FileMetadata.context STRING;
CREATE PROPERTY FileMetadata.md5 STRING;
CREATE PROPERTY FileMetadata.name STRING;
CREATE PROPERTY FileMetadata.date DATETIME;
CREATE PROPERTY FileMetadata.size LONG;
CREATE PROPERTY FileMetadata.zip BOOLEAN;
CREATE PROPERTY FileMetadata.chunksNumber INTEGER;
CREATE PROPERTY FileMetadata.properties EMBEDDEDMAP;
CREATE PROPERTY FileMetadata.autoClose BOOLEAN;
CREATE PROPERTY FileMetadata.status EMBEDDED FileStatus;

CREATE PROPERTY FileStatus.currentSize LONG;
CREATE PROPERTY FileStatus.chunksIndex EMBEDDEDSET INTEGER;
CREATE PROPERTY FileStatus.complete BOOLEAN;
CREATE PROPERTY FileStatus.error STRING;

CREATE PROPERTY ChunkMetadata.file LINK FileMetadata;
CREATE PROPERTY ChunkMetadata.index INTEGER;
CREATE PROPERTY ChunkMetadata.size LONG;
CREATE PROPERTY ChunkMetadata.uploaded BOOLEAN;



CREATE INDEX FileMetadata.id ON FileMetadata (context, md5) UNIQUE;
CREATE INDEX ChunkMetadata.id ON ChunkMetadata (file, index) UNIQUE;

CREATE INDEX FileMetadata.context NOTUNIQUE;



DISCONNECT;