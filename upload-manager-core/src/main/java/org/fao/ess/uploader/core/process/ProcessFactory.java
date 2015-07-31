package org.fao.ess.uploader.core.process;

import org.fao.ess.uploader.core.dto.FileMetadata;
import org.fao.ess.uploader.core.dto.ProcessMetadata;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.*;

public class ProcessFactory {
    @Inject private Instance<PreUpload> preInstance;
    @Inject private Instance<PostUpload> postInstance;

    public Collection<PreUpload> getPreUploadInstances(String context) {
        Collection<PreUpload> instances = new LinkedList<>();
        for (Iterator<PreUpload> instanceIterator = preInstance.select(PreUpload.class).iterator(); instanceIterator.hasNext();) {
            PreUpload instance = instanceIterator.next();
            ProcessInfo contextAnnotation = instance.getClass().getAnnotation(ProcessInfo.class);

            if (context==null || (contextAnnotation!=null && context.equals(contextAnnotation.context())))
                instances.add(instance);
        }
        return instances;
    }

    public Collection<ProcessMetadata> getPostUploadInstances(FileMetadata fileMetadata) {
        String context = fileMetadata.getContext();
        Collection<ProcessMetadata> instances = new LinkedList<>();
        List<Object[]> instancesIndex = new LinkedList<>();
        for (Iterator<PostUpload> instanceIterator = postInstance.select(PostUpload.class).iterator(); instanceIterator.hasNext();) {
            PostUpload instance = instanceIterator.next();
            ProcessInfo contextAnnotation = instance.getClass().getSuperclass().getAnnotation(ProcessInfo.class);

            if (context==null || (contextAnnotation!=null && context.equals(contextAnnotation.context()))) {
                ProcessMetadata metadata = new ProcessMetadata();
                metadata.setFile(fileMetadata);
                metadata.setName(contextAnnotation.name() != null ? contextAnnotation.name() : instance.getClass().getSuperclass().getSimpleName());
                metadata.instance(instance);
                instancesIndex.add(new Object[]{metadata, contextAnnotation.priority()});
            }
        }

        Collections.sort(instancesIndex, new Comparator<Object[]>() {
            @Override
            public int compare(Object[] o1, Object[] o2) {
                return ((Integer)o1[1]).compareTo((Integer)o2[1]);
            }
        });

        int i=0;
        for (Object[] instanceIndex : instancesIndex) {
            ProcessMetadata processMetadata = (ProcessMetadata) instanceIndex[0];
            processMetadata.setIndex(i++);
            instances.add(processMetadata);
        }

        return instances;
    }

}
