package org.fao.ess.uploader.core.upload;

import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class UploadFactory {
    @Inject private Instance<PreUpload> preInstance;
    @Inject private Instance<PostUpload> postInstance;

    public Collection<PreUpload> getPreUploadInstances(String context) {
        Collection<PreUpload> instances = new LinkedList<>();
        for (Iterator<PreUpload> instanceIterator = preInstance.select(PreUpload.class).iterator(); instanceIterator.hasNext();) {
            PreUpload instance = instanceIterator.next();
            UploadContext contextAnnotation = instance.getClass().getAnnotation(UploadContext.class);

            if (context==null || (contextAnnotation!=null && context.equals(contextAnnotation.value())))
                instances.add(instance);
        }
        return instances;
    }

    public Collection<PostUpload> getPostUploadInstances(String context) {
        Collection<PostUpload> instances = new LinkedList<>();
        for (Iterator<PostUpload> instanceIterator = postInstance.select(PostUpload.class).iterator(); instanceIterator.hasNext();) {
            PostUpload instance = instanceIterator.next();
            UploadContext contextAnnotation = instance.getClass().getAnnotation(UploadContext.class);

            if (context==null || (contextAnnotation!=null && context.equals(contextAnnotation.value())))
                instances.add(instance);
        }
        return instances;
    }

}
