package co.bitshfted.xapps.zsync.internal.util;

import co.bitshfted.xapps.zsync.internal.EventDispatcher;

import java.net.http.HttpResponse;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Created by gurkengewuerz.de on 23.04.2020.
 */
public class HTTPObserver implements HttpResponse.BodySubscriber<byte[]> {

    private long total = 0L;
    private long contentSize = 0L;
    private final EventDispatcher events;
    private HttpResponse.BodySubscriber<byte[]> subscriber;

    public HTTPObserver(HttpResponse.BodySubscriber<byte[]> subscriber, long contentSize, EventDispatcher events) {
        this.subscriber = subscriber;
        this.contentSize = contentSize;
        this.events = events;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        subscriber.onSubscribe(subscription);
    }

    @Override
    public void onNext(List<ByteBuffer> item) {
        long downloaded = item.stream().mapToLong(Buffer::remaining).sum();
        events.download(downloaded);
        total += downloaded;
        subscriber.onNext(item);
    }

    @Override
    public void onError(Throwable throwable) {
        subscriber.onError(throwable);
    }

    @Override
    public void onComplete() {
        subscriber.onComplete();
    }

    @Override
    public CompletionStage<byte[]> getBody() {
        return subscriber.getBody();
    }

    public long getTotal() {
        return total;
    }

    public long getContentSize() {
        return contentSize;
    }
}
