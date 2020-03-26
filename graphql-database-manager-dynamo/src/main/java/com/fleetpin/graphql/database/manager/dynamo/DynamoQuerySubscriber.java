package com.fleetpin.graphql.database.manager.dynamo;

import com.fleetpin.graphql.database.manager.Query;
import com.fleetpin.graphql.database.manager.Table;
import com.fleetpin.graphql.database.manager.util.CompletableFutureCollector;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DynamoQuerySubscriber implements Subscriber<QueryResponse> {
    private final ArrayList<DynamoItem> stuff;
    private final  AtomicInteger togo;
    private Subscription s;
    private final CompletableFuture<List<DynamoItem>> future = new CompletableFuture<List<DynamoItem>>();
    private final String table;

    protected DynamoQuerySubscriber(String table) {
        this(table, null);
    }

    protected DynamoQuerySubscriber(String table, Integer limit) {
        this.table = table;

        if (limit != null) {
            this.togo = new AtomicInteger(limit);
            this.stuff= new ArrayList<>(limit);
        } else {
            this.togo = null;
            this.stuff = new ArrayList<>();
        }
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.s = s;
        s.request(1);
    }

    @Override
    public void onNext(QueryResponse r) {
        try {
            var stream = r.items().stream();

            if (togo != null) {
                stream = stream.takeWhile(__ -> togo.getAndDecrement() >= 0);
            }

            stream.map(item -> new DynamoItem(this.table, item)).forEach(stuff::add);

            if (togo == null || togo.get() > 0) {
                this.s.request(1);
            } else {
                s.cancel();
                this.onComplete();
            }
        } catch (Exception e) {
            this.onError(e);
            s.cancel();
        }
    }

    @Override
    public void onError(Throwable t) {
        future.completeExceptionally(t);
    }

    @Override
    public void onComplete() {
        future.complete(stuff);
    }

    public CompletableFuture<List<DynamoItem>> getFuture() {
        return this.future;
    }
}
