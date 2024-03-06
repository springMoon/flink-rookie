package com.venn.question.stock.entry;

import java.math.BigDecimal;

/**
 * @Classname StockListDetail
 * @Description TODO
 * @Date 2023/6/8
 * @Created by venn
 */
public class StockListDetail {

   private int id;
    private long createTime;
    private String outStockCode;
    private String productCode;
    private String createOp;
    private BigDecimal outStockNum;

    public StockListDetail() {
    }

    public StockListDetail(int id, long createTime, String outStockCode, String productCode, String createOp, BigDecimal outStockNum) {
        this.id = id;
        this.createTime = createTime;
        this.outStockCode = outStockCode;
        this.productCode = productCode;
        this.createOp = createOp;
        this.outStockNum = outStockNum;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getOutStockCode() {
        return outStockCode;
    }

    public void setOutStockCode(String outStockCode) {
        this.outStockCode = outStockCode;
    }

    public String getProductCode() {
        return productCode;
    }

    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    public String getCreateOp() {
        return createOp;
    }

    public void setCreateOp(String createOp) {
        this.createOp = createOp;
    }

    public BigDecimal getOutStockNum() {
        return outStockNum;
    }

    public void setOutStockNum(BigDecimal outStockNum) {
        this.outStockNum = outStockNum;
    }

    @Override
    public String toString() {
        return "StockListDetail{" +
                "id=" + id +
                ", createTime=" + createTime +
                ", outStockCode='" + outStockCode + '\'' +
                ", productCode='" + productCode + '\'' +
                ", createOp='" + createOp + '\'' +
                ", outStockNum=" + outStockNum +
                '}';
    }
}
