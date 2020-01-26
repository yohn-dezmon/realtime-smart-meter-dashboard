public class DataRecord {
    /* Custom Java Object to be converted into a Json node for the Kafka Producer */
    private String dateTime;
    private String geohash;
    private String energyVal;

    public DataRecord() {

    }

    public DataRecord(String dateTime, String geohash, String energyVal) {
        this.dateTime = dateTime;
        this.geohash = geohash;
        this.energyVal = energyVal;
    }

    public String getDate() {
        return dateTime;
    }

    public void setDate(String dateTime) {
        this.dateTime = dateTime;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public String getEnergyVal() {
        return energyVal;
    }

    public void setEnergyVal(String energyVal) {
        this.energyVal = energyVal;
    }

}
