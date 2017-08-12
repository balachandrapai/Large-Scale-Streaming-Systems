import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.Before;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import java.io.*;
import java.util.Properties;


/**
 * @author likith,pai
 * Test class to run the Kafka broker, streaming applications and measuring latency 
 * and throughput values 
 */
public class KafkaTests {

	KafkaServerStartable kafka = null;
	HBaseTestingUtility htu = null;
	String zookeeperHost = "";
	String zookeeperPort = "";
	Properties kafkaProps = null;
	String zookeeperConnect;

	/**
	 * start ZK and Kafka Broker.
	 */
	@Before
	public void initialize() {
		System.setProperty("hadoop.home.dir", "c:/winutils/");

		String path = "/tmp/kafka-logs";
		final File kafkaLogs = new File(path);
		try {
			FileUtils.deleteDirectory(kafkaLogs);
		} catch (IOException e) {
			e.printStackTrace();
		}

		htu = HBaseTestingUtility.createLocalHTU();
		try {
			htu.cleanupTestDir();
			htu.startMiniZKCluster();

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

		Configuration hbaseConf = htu.getConfiguration();

		zookeeperHost = hbaseConf.get("hbase.zookeeper.quorum");
		zookeeperPort = hbaseConf.get("hbase.zookeeper.property.clientPort");

		zookeeperConnect = String.format("%s:%s", zookeeperHost, zookeeperPort);

		kafkaProps = new Properties();
		kafkaProps.put("broker.id", "1");
		kafkaProps.put("bootstrap.servers", "localhost:9092");
		kafkaProps.put("zookeeper.connect", zookeeperConnect);
		kafkaProps.put("client.id", "KafkaSuite");
		kafkaProps.put("group.id", "test-group");
		kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		kafkaProps.setProperty("delete.topic.enable", "true");
		kafkaProps.setProperty("auto.offset.reset", "earliest");

		KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
		kafka = new KafkaServerStartable(kafkaConfig);
		kafka.startup();
	}

	/**
	 * test directory mode of the trip generator.
	 */
	@Test
	public void someKafkaTest() {

		try {

//			SparkStreamingJSonJob.main(new String[] { zookeeperConnect, "my-consumer-group", "test", "1" });

			//Thread controlling the Spark streaming
			Thread sparkStreamerThread = new Thread(
					new SparkStreamingJSonJob(new String[] { zookeeperConnect, "my-consumer-group", "test", "1" }),
					"spark-streaming");
			sparkStreamerThread.start();

			//Thread to start the producer
			Thread producerThread = new Thread(new KafkaJSonProducer(), "producer");
			producerThread.start();
			
			//current kafkaTest thread to sleep for 1 second
			Thread.sleep(60000);

			sparkStreamerThread.interrupt();
			producerThread.interrupt();

			//flinkStreamerThread.stop();

			int sparkAccVal = SparkStreamingJSonJob.getAccumulator().intValue();
			System.out.println("Spark Throughput value : " + sparkAccVal);

			// ******************************************************************************************

//			FlinkStreamingJob.main(new String[] { "--topic", "test", "--bootstrap.servers", "localhost:9092",
//					"--zookeeper.connect", "zookeeperConnect", "--group.id", "my-consumer-group" });

			//Thread controlling the flink streaming
			/*Thread flinkStreamerThread = new Thread(
					new FlinkStreamingJob(new String[] { "--topic", "test", "--bootstrap.servers", "localhost:9092",
							"--zookeeper.connect", "zookeeperConnect", "--group.id", "my-consumer-group" }),
					"flink-streaming");
			flinkStreamerThread.start();*/

			//Thread to start the producer
//			Thread producerThread = new Thread(new KafkaJSonProducer(), "producer");
//			producerThread.start();

			//current kafkaTest thread to sleep for 1 second
//			Thread.sleep(60000);

//			flinkStreamerThread.interrupt();
//			producerThread.interrupt();

//			long flinkAccVal = FlinkStreamingJob.getmessagesCounter().getLocalValue().intValue();
//			System.out.println("Flink Throughput value : " + flinkAccVal);
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail();
		}

	}

	/**
	 * shutdown ZK and Kafka Broker.
	 */
	@After
	public void tearDown() {
		kafka.shutdown();

		try {
			htu.shutdownMiniZKCluster();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}