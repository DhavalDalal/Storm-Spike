package first;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class WordTopology {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new InMemoryWordSpout(), 2);
        builder.setBolt("upperCase", new UpperCaseBolt(), 2).shuffleGrouping("word");
        builder.setBolt("show", new ShowBolt(), 1).shuffleGrouping("upperCase");

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.setDebug(true);

        cluster.submitTopology("wordTopology", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("wordTopology");
    }
}
