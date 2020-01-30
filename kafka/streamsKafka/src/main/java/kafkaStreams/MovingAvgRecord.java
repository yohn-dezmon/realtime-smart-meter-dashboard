package kafkaStreams;

import java.sql.Timestamp;

public class MovingAvgRecord {

    /* Custom Java Object to be converted into a Json node for the Kafka Producer */
    private Timestamp dateTime;
    private Double movingAvg;
    private boolean energyTheft;
    private boolean outage;

    public MovingAvgRecord() {

    }

    public MovingAvgRecord(Timestamp dateTime, Double movingAvg,
                           boolean energyTheft,
                           boolean outage) {
        this.dateTime = dateTime;
        this.movingAvg = movingAvg;
        this.energyTheft = energyTheft;
        this.outage = outage;
    }

    public Timestamp getDate() {
        return dateTime;
    }

    public void setDate(Timestamp dateTime) {
        this.dateTime = dateTime;
    }

    public Double getMovingAvg() {
        return movingAvg;
    }

    public void setMovingAvg(Double movingAvg) {
        this.movingAvg = movingAvg;
    }

    public boolean getEnergyTheft() {
        return energyTheft;
    }

    public void setEnergyTheft(boolean energyTheft) {
        this.energyTheft = energyTheft;
    }

    public boolean getOutage() {
        return outage;
    }

    public void setOutage(boolean outage) {
        this.outage = outage;
    }
}
