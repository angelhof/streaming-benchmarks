/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package flink.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.Utils;
import edu.upenn.diffstream.EmptyDependence;
import edu.upenn.diffstream.FullDependence;
import edu.upenn.diffstream.matcher.MatcherFactory;
import edu.upenn.diffstream.monitor.ResourceMonitor;
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

import javax.xml.crypto.Data;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * To Run:  flink run target/flink-benchmarks-0.1.0-AdvertisingTopologyNative.jar  --confPath "../conf/benchmarkConf.yaml"
 */
public class AdvertisingTopologyNative {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTopologyNative.class);


    public static void main(final String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);


        Map conf = Utils.findAndReadConfigFile(parameterTool.getRequired("confPath"), true);
        int kafkaPartitions = ((Number) conf.get("kafka.partitions")).intValue();
        int hosts = ((Number) conf.get("process.hosts")).intValue();
        int cores = ((Number) conf.get("process.cores")).intValue();


        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(getFlinkConfs(conf));

        LOG.info("conf: {}", conf);
        LOG.info("Parameters used: {}", flinkBenchmarkParams.toMap());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(flinkBenchmarkParams);

        env.setParallelism(2);


        // This should be the same for stream for both tests, as it is the input
        DataStream<String> messageStream = env
                .addSource(new FlinkKafkaConsumer08<String>(
                        flinkBenchmarkParams.getRequired("topic"),
                        new SimpleStringSchema(),
                        flinkBenchmarkParams.getProperties())).setParallelism(Math.min(hosts * cores, kafkaPartitions));


        TimeUnit.SECONDS.sleep(5);

        // Uncomment any of the following scenarios.

        // This executes one pipeline (In order to find the maximum manageable throughput for it in our system)
        // executeOnePipeline(env, messageStream);

        // This executes two pipeline (In order to find the maximum manageable throughput for them in our system)
        // executeTwoPipelines(env, messageStream);

        // This just sends the output of one pipeline to two sinks. It is not really useful in the final evaluation
        // compareOnePipeline(env, messageStream);

        // This compares the outputs of two pipelines. It is used in the final evaluation.
        compareTwoPipelines(env, messageStream);

    }

    public static void executeOnePipeline(StreamExecutionEnvironment env, DataStream<String> input) throws Exception {
        // Computation
        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream = computation(input);

        // intermediateStream.print();

        intermediateStream.flatMap(new CampaignProcessor());
        env.execute();
    }

    public static void executeTwoPipelines(StreamExecutionEnvironment env, DataStream<String> input) throws Exception {
        // Computation
        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream1 = computation(input);
        intermediateStream1.flatMap(new CampaignProcessor());

        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream2 = computation(input);
        intermediateStream2.flatMap(new CampaignProcessor());

        env.execute();
    }

    public static void compareOnePipeline(StreamExecutionEnvironment env, DataStream<String> input) throws Exception {
        // Computation
        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream = computation(input);
        intermediateStream.flatMap(new CampaignProcessor());

        compareOutputs(intermediateStream, intermediateStream, env);
    }

    public static void compareTwoPipelines(StreamExecutionEnvironment env, DataStream<String> input) throws Exception {
        // Computation
        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream1 = computation(input, true);
        intermediateStream1.flatMap(new CampaignProcessor());

        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream2 = computation(input);
        intermediateStream2.flatMap(new CampaignProcessor());

        compareOutputs(intermediateStream1, intermediateStream2, env);
    }

    // This one has to call the intermediate pipeline again, so that there are indeed 2 pipelines running at the same time
    public static void compareOutputs(KeyedStream<Tuple3<String, String, String>, Tuple> leftStream,
                                      KeyedStream<Tuple3<String, String, String>, Tuple> rightStream,
                                      StreamExecutionEnvironment env) throws Exception {
        MatcherFactory.initRemote();
        try (var matcher = MatcherFactory.createRemoteMatcher(leftStream, rightStream, new EmptyDependence<Tuple3<String, String, String>>());
             var monitor = new ResourceMonitor(matcher, "unmatched-items.txt", "memory-log.txt")) {
            monitor.start();
            env.execute();
            matcher.assertStreamsAreEquivalent();
        }
        MatcherFactory.destroyRemote();
    }

    public static KeyedStream<Tuple3<String, String, String>, Tuple> computation(DataStream<String> inputStream) {

        return computation(inputStream, false);
    }

    public static KeyedStream<Tuple3<String, String, String>, Tuple> computation(DataStream<String> inputStream, boolean isSequential) {
        KeyedStream<Tuple3<String, String, String>, Tuple> intermediateStream;
        if (isSequential) {
            intermediateStream =
                    inputStream
                            .rebalance()
                            // Parse the String as JSON
                            .flatMap(new DeserializeBolt()).forceNonParallel()

                            //Filter the records if event type is "view"
                            .filter(new EventFilterBolt()).forceNonParallel()

                            // project the event
                            .<Tuple2<String, String>>project(2, 5).forceNonParallel()

                            // perform join with redis data
                            .flatMap(new RedisJoinBolt()).forceNonParallel()

                            // process campaign
                            .keyBy(0);

        } else {
            intermediateStream =
                    inputStream
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
        }
        return intermediateStream;
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
            if (campaign_id == null) {
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
            String event_time = tuple.getField(2);
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
        if (!conf.containsKey("zookeeper.servers")) {
            throw new IllegalArgumentException("Not zookeeper servers found!");
        }
        return listOfStringToString((List<String>) conf.get("zookeeper.servers"), String.valueOf(conf.get("zookeeper.port")));
    }

    private static String getKafkaBrokers(Map conf) {
        if (!conf.containsKey("kafka.brokers")) {
            throw new IllegalArgumentException("No kafka brokers found!");
        }
        if (!conf.containsKey("kafka.port")) {
            throw new IllegalArgumentException("No kafka port found!");
        }
        return listOfStringToString((List<String>) conf.get("kafka.brokers"), String.valueOf(conf.get("kafka.port")));
    }

    private static String getKafkaTopic(Map conf) {
        if (!conf.containsKey("kafka.topic")) {
            throw new IllegalArgumentException("No kafka topic found!");
        }
        return (String) conf.get("kafka.topic");
    }

    private static String getRedisHost(Map conf) {
        if (!conf.containsKey("redis.host")) {
            throw new IllegalArgumentException("No redis host found!");
        }
        return (String) conf.get("redis.host");
    }

    public static String listOfStringToString(List<String> list, String port) {
        String val = "";
        for (int i = 0; i < list.size(); i++) {
            val += list.get(i) + ":" + port;
            if (i < list.size() - 1) {
                val += ",";
            }
        }
        return val;
    }

}
