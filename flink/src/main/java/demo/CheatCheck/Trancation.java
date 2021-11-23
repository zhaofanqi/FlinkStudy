package demo.CheatCheck;

import java.util.Date;

/**
 * 输入数据模板
 */
public class Trancation {
    private int id;
    private String ts;
    private Double amount;

    public Trancation(int id, String ts, Double amount) {
        this.id = id;
        this.ts = ts;
        this.amount = amount;
    }

    public Trancation() {
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }


    @Override
    public String toString() {
        return "Trancation{" +
                "id=" + id +
                ", ts='" + ts + '\'' +
                ", amount=" + amount +
                '}';
    }
}
