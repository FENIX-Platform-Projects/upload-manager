package org.fao.ess.uploader.dist.services;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.NoContentException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class DefaultErrorManager implements ExceptionMapper<Throwable> {
    @Override
    public Response toResponse(Throwable e) {

        if (e instanceof NoContentException)
            return Response.noContent().entity(e.getMessage()).build();
        else if (e instanceof WebApplicationException) {
            e.printStackTrace();
            return e.getCause() != null ? Response.serverError().entity(e.getCause().getMessage()).build() : ((WebApplicationException) e).getResponse();
        } else {
            e.printStackTrace();
            return Response.serverError().entity(e.getMessage()).build();
        }
    }
}
