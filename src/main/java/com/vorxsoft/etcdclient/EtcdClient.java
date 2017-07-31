package com.vorxsoft.etcdclient;


import com.google.protobuf.ByteString;
//import com.sun.org.apache.xml.internal.security.keys.content.KeyValue;
import com.vorxsoft.jetcd.api.*;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
//import org.w3c.dom.ranges.Range;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class EtcdClient {

    public EtcdClient(String host, int port) {
        HOST = host;
        PORT = port;
        managedChannel = NettyChannelBuilder.forAddress(HOST, PORT).usePlaintext(true).build();
        _kvStub = KVGrpc.newBlockingStub(managedChannel);
        _leaseBlockingStub = LeaseGrpc.newBlockingStub(managedChannel);
        _leaseStub = LeaseGrpc.newStub(managedChannel);
        _watchStub = WatchGrpc.newStub(managedChannel);
    }

    public EtcdClient() {
        managedChannel = NettyChannelBuilder.forAddress(HOST, PORT).usePlaintext(true).build();
        _kvStub = KVGrpc.newBlockingStub(managedChannel);
        _leaseBlockingStub = LeaseGrpc.newBlockingStub(managedChannel);
        _leaseStub = LeaseGrpc.newStub(managedChannel);
        _watchStub = WatchGrpc.newStub(managedChannel);
    }


    private String setRangeEnd(String key) {
        char[] range_end = key.toCharArray();
        char ascii = (char) (range_end[range_end.length - 1] + 1);
        range_end[range_end.length - 1] = ascii;

        return range_end.toString();
    }


    public String get(String key) {
        RangeRequest req = RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).build();

        RangeResponse reply = _kvStub.range(req);
        return reply.getKvs(1).toString();

    }

    public void set(String key, String value) {
        PutResponse reply = _kvStub.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setValue(ByteString.copyFromUtf8(value)).build());

    }

    public void set(String key, String value, int ttl) {
        long leaseId = leaseGrant(ttl);
        PutResponse reply = _kvStub.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setValue(ByteString.copyFromUtf8(value)).
                setLease(leaseId).build());
    }

    public void set(String key, String value, long leaseId) {
        PutResponse reply = _kvStub.put(PutRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setValue(ByteString.copyFromUtf8(value)).
                setLease(leaseId).build());
    }

    public java.util.List ls(String key) {
        RangeRequest req = RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setRangeEnd(ByteString.copyFromUtf8(setRangeEnd(key))).build();
        RangeResponse reply = _kvStub.range(req);
        return reply.getKvsList();
    }

    public int lsNumber(String key) {
        RangeRequest req = RangeRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setRangeEnd(ByteString.copyFromUtf8(setRangeEnd(key))).build();
        RangeResponse reply = _kvStub.range(req);
        return reply.getKvsList().size();
    }

    public void rm(String key) {
        DeleteRangeRequest req = DeleteRangeRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).build();
        DeleteRangeResponse reply = _kvStub.deleteRange(req);
        System.out.println("has " + reply.getDeleted() + " key deleted");
    }

    public void rmdir(String key) {
        DeleteRangeRequest req = DeleteRangeRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).
                setRangeEnd(ByteString.copyFromUtf8(setRangeEnd(key))).
                build();
        DeleteRangeResponse reply = _kvStub.deleteRange(req);
        System.out.println("has " + reply.getDeleted() + " key deleted");
    }

    private void shutdown() {
        if (managedChannel != null) {
            try {
                managedChannel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public long leaseGrant(int ttl) {
        LeaseGrantResponse leaseGrantResponse = _leaseBlockingStub.leaseGrant(LeaseGrantRequest.newBuilder().setTTL(ttl).build());
        return leaseGrantResponse.getID();
    }

    public void leaseKeepAlive(int ttl) {
        leaseKeepAlive(leaseGrant(ttl), ttl - 1);
    }

    private void leaseKeepAlive(long leaseId, int ttl) {
        StreamObserver<LeaseKeepAliveRequest> leaseKeepAliveRequestStreamObserver = _leaseStub.leaseKeepAlive(new StreamObserver<LeaseKeepAliveResponse>() {

            @Override
            public void onNext(LeaseKeepAliveResponse value) {
                System.out.println("LeaseKeepAliveResponse  value" + value);
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("onCompleted  !!!");
            }
        });

        _s.scheduleAtFixedRate(() -> {
            leaseKeepAliveRequestStreamObserver.onNext(LeaseKeepAliveRequest.newBuilder().setID(leaseId).build());
        }, 1l, ttl, TimeUnit.SECONDS);
    }

    public void setWatcher(String key) {
        StreamObserver<WatchRequest> requestStreamObserver = _watchStub.watch(new StreamObserver<WatchResponse>() {
            @Override
            public void onNext(WatchResponse value) {
                System.out.println(value.toString());
                System.out.println("有变化------------------");
            }

            @Override
            public void onError(Throwable t) {
                t.printStackTrace();
            }

            @Override
            public void onCompleted() {
                System.out.println("完成");
            }
        });

        WatchRequest watchRequest = WatchRequest.newBuilder().setCreateRequest(WatchCreateRequest.newBuilder().setKey(ByteString.copyFromUtf8(key)).build()).build();
        requestStreamObserver.onNext(watchRequest);


        //TimeUnit.HOURS.sleep(1);
    }

    public KVGrpc.KVBlockingStub get_kvStub() {
        return _kvStub;
    }

    public WatchGrpc.WatchStub get_watchStub() {
        return _watchStub;
    }

    public LeaseGrpc.LeaseBlockingStub get_BlockingleaseStub() {
        return _leaseBlockingStub;
    }

    public LeaseGrpc.LeaseStub get_leaseStub() {
        return _leaseStub;
    }

    private KVGrpc.KVBlockingStub _kvStub;
    private LeaseGrpc.LeaseBlockingStub _leaseBlockingStub;
    private LeaseGrpc.LeaseStub _leaseStub;
    private WatchGrpc.WatchStub _watchStub;

    private ManagedChannel managedChannel;
    private String HOST = "localhost";
    private int PORT = 2359;
    private ScheduledExecutorService _s = Executors.newScheduledThreadPool(2);
}
