package evaluation;

import java.time.Instant;

public class TimeMeasurement {

    public static Instant getCurrentTimeStamp() {
        return Instant.now();
    }

    private Instant startTime;
    private Instant endTime;

    /**
     * starts measuring or returns the start timestamp if the measuring has already started.
     * @return the start time
     */
    public Instant startMeasurement() {
        if (startTime == null) {
            startTime = getCurrentTimeStamp();
        }
        return startTime;
    }

    /**
     * stopps the time measurement or returns the end timestamp if the measuring has already been stopped.
     * @return tje end time
     */
    public Instant endMeasurement() {
        if (endTime == null) {
            endTime = getCurrentTimeStamp();
        }
        return endTime;
    }

    /**
     * @return the measured time in seconds
     */
    public long getTimeDiferenceInSeconds() {
        return endTime.getEpochSecond() - startTime.getEpochSecond();
    }

    /**
     * @return the measured time in millis
     */
    public long getTimeDiferenceInMillis() {
        return endTime.toEpochMilli() - startTime.toEpochMilli();
    }
}
