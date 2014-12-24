package tophashtagsmap.utils;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.concurrent.LinkedBlockingQueue;

public class GeoTwitterListener implements StatusListener {

    private final LinkedBlockingQueue<String> queue;

    public GeoTwitterListener(LinkedBlockingQueue<String> queue) {
        this.queue = queue;
    }

    // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status) {
        String lat = "0", lon = "0";
        if (status.getGeoLocation() != null) {
            lat = "" + status.getGeoLocation().getLatitude();
            lon = "" + status.getGeoLocation().getLongitude();
        }
        queue.offer(lat + "|" + lon + "|" + status.getText());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice sdn) {
    }

    @Override
    public void onTrackLimitationNotice(int i) {
    }

    @Override
    public void onScrubGeo(long l, long l1) {
    }

    @Override
    public void onStallWarning(StallWarning warning) {
    }

    @Override
    public void onException(Exception e) {
        e.printStackTrace();
    }
};
