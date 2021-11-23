package demo.CheatCheck;

public class Alert {
    private int aid;

    public Alert() {
    }

    public int getAid() {
        return aid;
    }

    public void setAid(int aid) {
        this.aid = aid;
    }

    public Alert(int id) {
        this.aid = id;
    }



    @Override
    public String toString() {
        return "Alert{" +
                "id=" + aid +
                '}';
    }
}
