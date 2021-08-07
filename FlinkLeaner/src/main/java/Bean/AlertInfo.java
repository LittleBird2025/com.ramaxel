package Bean;

//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//
//@Data
//@NoArgsConstructor
//@AllArgsConstructor
public class AlertInfo {
    private String timeStrap;
    private String serverId;
    private String location;
    private Long alarmCount;

    public AlertInfo(){

    }
    public AlertInfo(String timeStrap, String serverId, String location, Long alarmCount) {
        this.timeStrap = timeStrap;
        this.serverId = serverId;
        this.location = location;
        this.alarmCount = alarmCount;
    }

    public String getTimeStrap() {
        return timeStrap;
    }

    public void setTimeStrap(String timeStrap) {
        this.timeStrap = timeStrap;
    }

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public Long getAlarmCount() {
        return alarmCount;
    }

    public void setAlarmCount(Long alarmCount) {
        this.alarmCount = alarmCount;
    }

    @Override
    public String toString() {
        return "AlertInfo{" +
                "timeStrap='" + timeStrap + '\'' +
                ", serverId='" + serverId + '\'' +
                ", location='" + location + '\'' +
                ", alarmCount=" + alarmCount +
                '}';
    }
}
