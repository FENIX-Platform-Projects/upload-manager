package org.fao.ess.uploader.core.process;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProcessInfo {
    String context();
    String name();
    int priority() default Integer.MAX_VALUE;
}
