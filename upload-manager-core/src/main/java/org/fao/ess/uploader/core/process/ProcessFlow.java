package org.fao.ess.uploader.core.process;

import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.ProcessMetadata;
import org.fao.ess.uploader.core.metadata.MetadataStorage;
import org.fao.ess.uploader.core.storage.BinaryStorage;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class ProcessFlow implements Runnable {
    private MetadataStorage metadataStorage;
    private BinaryStorage binaryStorage;
    private FileMetadata fileMetadata;

    private Collection<ProcessMetadata> flow;
    private Map<String, Object> processingParams;

    private long delay;


    public ProcessFlow(MetadataStorage metadataStorage, BinaryStorage binaryStorage, FileMetadata fileMetadata, Collection<ProcessMetadata> flow, Map<String, Object> processingParams) {
        this.metadataStorage = metadataStorage;
        this.binaryStorage = binaryStorage;
        this.fileMetadata = fileMetadata;
        this.flow = flow;
        this.processingParams = processingParams;
    }

    public void start(long delay) {
        this.delay = delay>0 ? delay : -1;
        Thread t = new Thread(this);
        t.start();
    }

    @Override
    public void run() {
        if (delay>0)
            try {
                Thread.currentThread().sleep(delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        ProcessMetadata currentProcess = null;
        try {
            for (Iterator<ProcessMetadata> processIterator=flow.iterator(); processIterator.hasNext();) {
                (currentProcess=processIterator.next()).instance().fileUploaded(fileMetadata, binaryStorage,processingParams);
                currentProcess.setCompleted(true);
                metadataStorage.save(currentProcess);
            }
        } catch (Exception ex) {
            if (currentProcess!=null)
                try {
                    currentProcess.setCompleted(false);
                    currentProcess.setError(ex.getMessage());
                    metadataStorage.save(currentProcess);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            ex.printStackTrace();
        }
    }
}
