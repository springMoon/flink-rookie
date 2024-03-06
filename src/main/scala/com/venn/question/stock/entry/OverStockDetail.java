package com.venn.question.stock.entry;

import java.math.BigDecimal;

/**
 * @Classname OverStockDetail
 * @Description TODO
 * @Date 2023/6/8
 * @Created by venn
 */
public class OverStockDetail implements Stock{

    private int id;
    private String fid;
    private String fentryId;
    private String fmaterialId;
    private BigDecimal frealQty;

    public OverStockDetail() {
    }

    public OverStockDetail(int id, String fid, String fentryid, String fmaterialid, BigDecimal frealqty) {
        this.id = id;
        this.fid = fid;
        this.fentryId = fentryid;
        this.fmaterialId = fmaterialid;
        this.frealQty = frealqty;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getFentryId() {
        return fentryId;
    }

    public void setFentryId(String fentryId) {
        this.fentryId = fentryId;
    }

    public String getFmaterialId() {
        return fmaterialId;
    }

    public void setFmaterialId(String fmaterialId) {
        this.fmaterialId = fmaterialId;
    }

    public BigDecimal getFrealQty() {
        return frealQty;
    }

    public void setFrealQty(BigDecimal frealQty) {
        this.frealQty = frealQty;
    }

    @Override
    public String toString() {
        return "OverStockDetail{" +
                "id=" + id +
                ", fid='" + fid + '\'' +
                ", fentryId='" + fentryId + '\'' +
                ", fmaterialId='" + fmaterialId + '\'' +
                ", frealQty=" + frealQty +
                '}';
    }
}
