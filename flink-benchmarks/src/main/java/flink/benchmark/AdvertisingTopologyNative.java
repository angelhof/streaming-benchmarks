/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.Utils;
import edu.upenn.streamstesting.EmptyDependence;
import edu.upenn.streamstesting.FullDependence;
import edu.upenn.streamstesting.remote.RemoteMatcherFactory;
import edu.upenn.streamstesting.remote.RemoteStreamEquivalenceMatcher;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.upenn.streamstesting.StreamEquivalenceMatcher;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);


    public static void main(final String[] args) throws Exception {

        System.out.println("I am done pipi");

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        System.out.println("I am done pipi 2");


        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        int kafkaPartitions = ((Number)conf.get("kafka.partitions")).intValue();
        int hosts = ((Number)conf.get("process.hosts")).intValue();
        int cores = ((Number)conf.get("process.cores")).intValue();

        System.out.println("I am done pipi 3");


        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        System.out.println("I am done pipi 3");


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

		// Set the buffer timeout (default 100)
        // Lowering the timeout will lead to lower latencies, but will eventually reduce throughput.
//        env.setBufferTimeout(flinkBenchmarkParams.getLong("flink.buffer-timeout", 100));
//
//        if(flinkBenchmarkParams.has("flink.checkpoint-interval")) {
//            // enable checkpointing for fault tolerance
//            env.enableCheckpointing(flinkBenchmarkParams.getLong("flink.checkpoint-interval", 1000));
//        }

        // WARNING: There is a problem with the parallelism. If I uncomment the following the benchmark doesn't run at all.
        // set default parallelism for all operators (recommended value: number of available worker CPU cores in the cluster (hosts * cores))
//        env.setParallelism(hosts * cores);
//        env.setParallelism(1);
        System.out.println("I am done pipi 4");


        // This should be the same for stream for both tests, as it is the input
        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer08<String>(
                        flinkBenchmarkParams.getRequired("topic"),
                        new SimpleStringSchema(),
                        flinkBenchmarkParams.getProperties())).setParallelism(Math.min(hosts * cores, kafkaPartitions));

        System.out.println("I am done pipi 5");

        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream =
                messageStream
                .rebalance()
                // Parse the String as JSON
                .flatMap(new DeserializeBolt())

                //Filter the records if event type is "view"
                .filter(new EventFilterBolt())

                // project the event
                .<Tuple2<String, String>>project(2, 5)

                // perform join with redis data
                .flatMap(new RedisJoinBolt())

                // process campaign
                .keyBy(0);

        // intermediateStream.print();


        // This can be used to test that the remote matcher indeed works.
//        KeyedStream<Tuple3<String, String, String>, Tuple> bad =
//                env.fromElements(new Tuple3<>("pipi", "popo", "pupu")).keyBy(0);

        // Here is where we will probe. Unfortunately, there is no ordering needed as it seems, as the event time
        // can be used to write in a window far back in the past..

        RemoteMatcherFactory.init();
        RemoteStreamEquivalenceMatcher<Tuple3<String, String, String>> matcher = RemoteMatcherFactory.getInstance().
                createMatcher(intermediateStream, intermediateStream, new FullDependence<>());

        // TODO: Can we change the implementation in a way that requires ordering? (It is clear that this implementation
        // doesn't provide any ordering though).

        intermediateStream.flatMap(new CampaignProcessor());

        // TODO: I could try to do some optimization on the campaignProcessor since the events that it gets are keyed by,
        // so all the events of the same campaign_id happen in the same node. Maybe we can ditch part of the synchronization
        // that there is in campaign processor?

        // VERY IMPORTANT NOTE: I shouldn't look at any optimizations in the last step, as it is very difficult to witness
        // them. I should just try to do optimizations before the last step. Or just do a test there.

        // Run a thread that will output memory measurements every 1 second.
        Runnable r = new Runnable() {
            public void run() {
                measureMemory();
            }
        };

        new Thread(r).start();

        // Execute and then ask the matcher if the outputs are equivalent
        env.execute();
        System.out.println("I am done pipi 6");
        matcher.assertStreamsAreEquivalent();
        System.out.println("I am done pipi 7");
        RemoteMatcherFactory.destroy();

    }

    // If we have issues with sleep drift, then we can use this method.
    // https://stackoverflow.com/questions/24104313/how-do-i-make-a-delay-in-java
    public static void measureMemory() {
        PrintWriter pw = null;

        // TODO: Debug why this doesn't work, when before it worked properly
        try {
            File file = new File("memory-log.txt");
            FileWriter fw = new FileWriter(file, true);
            pw = new PrintWriter(fw);
            pw.println("Hi :)");
            Runtime runtime = Runtime.getRuntime();
            long memory = getUsedMemory(runtime);
            pw.println("Used memory is bytes: " + memory);
            while(true) {
                memory = getUsedMemory(runtime);
                pw.println("Used memory is bytes: " + memory);
                TimeUnit.SECONDS.sleep(1);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            System.out.println(" !! !! [ERROR] Thread got interrupted!");
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }

    // Calculate the used memory
    public static long getUsedMemory(Runtime runtime) {
        return runtime.totalMemory() - runtime.freeMemory();
    }

    public static class DeserializeBolt implements
            FlatMapFunction<String, Tuple7<String, String, String, String, String, String, String>> {

        @Override
        public void flatMap(String input, Collector<Tuple7<String, String, String, String, String, String, String>> out)
                throws Exception {
            JSONObject obj = new JSONObject(input);
            Tuple7<String, String, String, String, String, String, String> tuple =
                    new Tuple7<String, String, String, String, String, String, String>(
                            obj.getString("user_id"),
                            obj.getString("page_id"),
                            obj.getString("ad_id"),
                            obj.getString("ad_type"),
                            obj.getString("event_type"),
                            obj.getString("event_time"),
                            obj.getString("ip_address"));
            out.collect(tuple);
        }
    }

    public static class EventFilterBolt implements
            FilterFunction<Tuple7<String, String, String, String, String, String, String>> {
        @Override
        public boolean filter(Tuple7<String, String, String, String, String, String, String> tuple) throws Exception {
            return tuple.getField(4).equals("view");
        }
    }

    public static final class RedisJoinBolt extends RichFlatMapFunction<Tuple2<String, String>, Tuple3<String, String, String>> {

        RedisAdCampaignCache redisAdCampaignCache;

        @Override
        public void open(Configuration parameters) {
            //initialize jedis
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache = new RedisAdCampaignCache(parameterTool.getRequired("jedis_server"));
            this.redisAdCampaignCache.prepare();
        }

        @Override
        public void flatMap(Tuple2<String, String> input,
                            Collector<Tuple3<String, String, String>> out) throws Exception {
            String ad_id = input.getField(0);
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }

            Tuple3<String, String, String> tuple = new Tuple3<String, String, String>(
                    campaign_id,
                    (String) input.getField(0),
                    (String) input.getField(1));
            out.collect(tuple);
        }
    }

    // Every 1 second, windows are flushed.

    // When we flush, we write all windows to redis.
    // We first get the windowID that is associated with a campaign.
    // A campaign id has many fields on redis, such as a window id for each quantized timestamp.

    // If there was no window associated with this quantum for this campaign id yet, we create it, giving it a random id
    // We also initialize (if it wasn't the case) a window list for each camping_id, which holds all ids of all windows


    // When execute is called, the campaign_id and the event_time is used to get a window.
    // The event_time is quantized to a time bucket.

    // The time bucket is used to get a hash map from campaigns to windows.
    // The time bucket seems to be like the epoch. Every epoch has a window for each campaidn_id

    // If the hash map for this epoch has not been initialized, we initialize it, and then
    // We search for the window in the hash map for the specific campaing_id, if it doesn't exist
    // we initialize that too.

    // After the window is returned, we increase the count by one.
    // So essentially we just count how many times each campaign has appeared in every time quantum

    // TODO: I probably need to package our framework and call it from here, rather than the other way around

    // AD events contain their event time and so ordering should be preserved.

    // This implementation here has a performance bottleneck on Redis, as all processes talk to it simultaneously.
    // However, since we just search for the window on Redis, we might be able to update a window with a much older
    // event time.
    //
    // However, since redis writes are side effects of the final flatmap, they might happen more than once in case
    // of faults. As flink cannot guarantee exactly once for the side effects of its operators.

    // In contrast the implementation that uses flink windows, writes on Redis using a flink sink,
    // so it makes sure that writes are written only once (if Flink is configured to guarantee exactly once).

    // Can Flink guarantee exactly once for all events and that they will be executed in their respective window?



    public static class CampaignProcessor extends RichFlatMapFunction<Tuple3<String, String, String>, String> {

        CampaignProcessorCommon campaignProcessorCommon;

        @Override
        public void open(Configuration parameters) {
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            parameterTool.getRequired("jedis_server");
            LOG.info("Opening connection with Jedis to {}", parameterTool.getRequired("jedis_server"));

            this.campaignProcessorCommon = new CampaignProcessorCommon(parameterTool.getRequired("jedis_server"));
            this.campaignProcessorCommon.prepare();
        }

        @Override
        public void flatMap(Tuple3<String, String, String> tuple, Collector<String> out) throws Exception {

            String campaign_id = tuple.getField(0);
            String event_time =  tuple.getField(2);
            // TODO: Understand if this requires any order
            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
    }

    private static Map<String, String> getFlinkConfs(Map conf) {
        String kafkaBrokers = getKafkaBrokers(conf);
        String zookeeperServers = getZookeeperServers(conf);

        Map<String, String> flinkConfs = new HashMap<String, String>();
        flinkConfs.put("topic", getKafkaTopic(conf));
        flinkConfs.put("bootstrap.servers", kafkaBrokers);
        flinkConfs.put("zookeeper.connect", zookeeperServers);
        flinkConfs.put("jedis_server", getRedisHost(conf));
        flinkConfs.put("group.id", "myGroup");

        return flinkConfs;
    }

    private static String getZookeeperServers(Map conf) {
        if(!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if(!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if(!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if(!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String)conf.get("kafka.topic");
    }

    private static String getRedisHost(Map conf) {
        if(!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String)conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for(int i=0; i<list.size(); i++) {
            val += list.get(i) + ":" + port;
            if(i < list.size()-1) {
                val += ",";
            }
        }
        return val;
    }
}
