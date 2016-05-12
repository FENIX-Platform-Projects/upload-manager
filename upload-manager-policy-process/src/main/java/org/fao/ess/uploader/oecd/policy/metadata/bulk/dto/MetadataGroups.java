package org.fao.ess.uploader.oecd.policy.metadata.bulk.dto;

import org.fao.fenix.commons.msd.dto.full.DSDDataset;
import org.fao.fenix.commons.msd.dto.full.MeIdentification;

import java.util.Collection;
import java.util.LinkedList;

public class MetadataGroups {
    public Collection<MeIdentification<DSDDataset>> insert = new LinkedList<>();
    public Collection<MeIdentification<DSDDataset>> update = new LinkedList<>();
    public Collection<MeIdentification<DSDDataset>> delete = new LinkedList<>();
}
