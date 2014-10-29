package first;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class WordTopology {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word", new InMemoryWordSpout(), 2);
        builder.setBolt("upperCase", new UpperCaseBolt(), 2).shuffleGrouping("word");
        builder.setBolt("show", new ShowBolt(), 1).shuffleGrouping("upperCase");
        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            System.out.println("Starting on Remote Cluster...");
            //args[0] = "wordTopology"
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setDebug(true);
            System.out.println("Starting on Local Cluster...");
            LocalCluster cluster = new LocalCluster();

            cluster.submitTopology("wordTopology", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("wordTopology");
        }
    }
}
