package com.venn.question.stock.entry;

/**
 * @Classname StockList
 * @Description TODO
 * @Date 2023/6/8
 * @Created by venn
 */
public class StockList {

    private int id;
    private long createTime;
    private String outStockCode;
    private String createOp;
    private String distributorCode;
    private String inoutType;

    public StockList() {
    }

    public StockList(int id, long fdate, String fid, String fbillno, String fcustomerid, String fGjzh) {
        this.id = id;
        this.createTime = fdate;
        this.outStockCode = fid;
        this.createOp = fbillno;
        this.distributorCode = fcustomerid;
        this.inoutType = fGjzh;
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

    public String getCreateOp() {
        return createOp;
    }

    public void setCreateOp(String createOp) {
        this.createOp = createOp;
    }

    public String getDistributorCode() {
        return distributorCode;
    }

    public void setDistributorCode(String distributorCode) {
        this.distributorCode = distributorCode;
    }

    public String getInoutType() {
        return inoutType;
    }

    public void setInoutType(String inoutType) {
        this.inoutType = inoutType;
    }

    @Override
    public String toString() {
        return "StockList{" +
                "id=" + id +
                ", createTime=" + createTime +
                ", outStockCode='" + outStockCode + '\'' +
                ", createOp='" + createOp + '\'' +
                ", distributorCode='" + distributorCode + '\'' +
                ", inoutType='" + inoutType + '\'' +
                '}';
    }
}
