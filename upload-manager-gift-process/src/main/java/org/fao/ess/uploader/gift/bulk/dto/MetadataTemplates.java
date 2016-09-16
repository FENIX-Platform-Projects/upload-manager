package org.fao.ess.uploader.gift.bulk.dto;

public enum MetadataTemplates {
    //gift_process_default(true, true),
    gift_process_daily_avg(true, false);

    public boolean bySurvey, byItem;
    MetadataTemplates(boolean bySurvey, boolean byItem) {
        this.bySurvey = bySurvey;
        this.byItem = byItem;
    }
}
