package com.yahoo.labs.slb;

import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * This is a basic example of a Storm topology.
 */
public class WordCountPartialKeyGrouping {

    public static class SplitterBolt extends BaseBasicBolt {
        private static final long serialVersionUID = -4094707939635564788L;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String tokens[] = tuple.getString(0).split("\t"); // assume tab separated key-value pair
            String word_tokens[] = tokens[1].split(" ");
            for (int i = 0; i < word_tokens.length; i++) {
                String str = word_tokens[i].trim().toLowerCase();
                if (str != null && !str.isEmpty())
                    collector.emit(new Values(str));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class CounterBolt extends BaseBasicBolt {
        private static final long serialVersionUID = -2350373680379322599L;
        private Map<String, Integer> counts = new HashMap<String, Integer>();
        private static final int DEFAULT_TICK_FREQUENCY_SECONDS = 10;

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            if (isTickTuple(tuple)) {
                emit(collector);
                counts = new HashMap<String, Integer>();
            } else {
                String word = tuple.getString(0);
                if (!word.isEmpty()) {
                    Integer count = counts.get(word);
                    if (count == null) {
                        count = 0;
                    }
                    count++;
                    counts.put(word, count);
                }
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, DEFAULT_TICK_FREQUENCY_SECONDS);
            return conf;
        }

        private static boolean isTickTuple(Tuple tuple) {
            return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
        }

        private void emit(BasicOutputCollector collector) {
            for (Map.Entry<String, Integer> entry : counts.entrySet()) {
                String str = entry.getKey();
                Integer count = entry.getValue();
                collector.emit(new Values(str, count));
            }
        }
    }

    public static class AggregatorBolt extends BaseBasicBolt {
        private static final long serialVersionUID = -1410983886447378438L;
        private Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getStringByField("word");
            if (!word.isEmpty()) {
                Integer delta_count = tuple.getIntegerByField("count");
                Integer count = counts.get(word);
                if (count == null)
                    count = 0;
                count = count + delta_count;
                counts.put(word, count);
                // collector.emit(new Values(word, count)); // optionally send the final counts downstream
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        String kafkaZk = "zookeeper:2181"; // change it to your zookeeper server
        BrokerHosts brokerHosts = new ZkHosts(kafkaZk);

        SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "name_of_kafka_topic", "", "test"); // change it to the name of your kafka topic
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("stream", new KafkaSpout(kafkaConfig), 1);
        builder.setBolt("split", new SplitterBolt(), 8).shuffleGrouping("stream");
        builder.setBolt("counter", new CounterBolt(), 10).customGrouping("split", new PartialKeyGrouping());
        builder.setBolt("aggregator", new AggregatorBolt(), 1).fieldsGrouping("counter", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(100);
        // conf.setMessageTimeoutSecs(300); // optionally increase the timeout for tuples

        if (args != null && args.length > 0) {
            conf.setNumWorkers(10);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(15000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
