package com.venn.question.stock.entry;

/**
 * @Classname OverStock
 * @Description TODO
 * @Date 2023/6/8
 * @Created by venn
 */
public class OverStock implements Stock{

    private int id;
    private long fdate;
    private String fid;
    private String fbillNo;
    private String fcustomerId;
    private String fGjzh;

    public OverStock() {
    }

    public OverStock(int id, long fdate, String fid, String fbillno, String fcustomerid, String fGjzh) {
        this.id = id;
        this.fdate = fdate;
        this.fid = fid;
        this.fbillNo = fbillno;
        this.fcustomerId = fcustomerid;
        this.fGjzh = fGjzh;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public long getFdate() {
        return fdate;
    }

    public void setFdate(long fdate) {
        this.fdate = fdate;
    }

    public String getFid() {
        return fid;
    }

    public void setFid(String fid) {
        this.fid = fid;
    }

    public String getFbillNo() {
        return fbillNo;
    }

    public void setFbillNo(String fbillNo) {
        this.fbillNo = fbillNo;
    }

    public String getFcustomerId() {
        return fcustomerId;
    }

    public void setFcustomerId(String fcustomerId) {
        this.fcustomerId = fcustomerId;
    }

    public String getfGjzh() {
        return fGjzh;
    }

    public void setfGjzh(String fGjzh) {
        this.fGjzh = fGjzh;
    }

    @Override
    public String toString() {
        return "OverStock{" +
                "id=" + id +
                ", fdate=" + fdate +
                ", fid='" + fid + '\'' +
                ", fbillNo='" + fbillNo + '\'' +
                ", fcustomerId='" + fcustomerId + '\'' +
                ", fGjzh='" + fGjzh + '\'' +
                '}';
    }
}
