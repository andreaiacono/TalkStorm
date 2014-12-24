package tophashtags.utils;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.concurrent.LinkedBlockingQueue;

public class TwitterListener implements StatusListener {

    private final LinkedBlockingQueue<String> queue;

    public TwitterListener(LinkedBlockingQueue<String> queue) {
        this.queue = queue;
    }

    // Implement the callback function when a tweet arrives
    @Override
    public void onStatus(Status status) {
        // add the tweet into the queue buffer
        queue.offer(status.getText());
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
