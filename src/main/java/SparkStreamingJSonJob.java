import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 
 * @author pai,likith
 *
 */
public class SparkStreamingJSonJob implements Runnable {

	private static Accumulator<Integer> messCounter;
	private static int count;
	private static String[] args = {};
	private static Map<Integer, Long> messTime = new HashMap<Integer, Long>();
	public static JavaStreamingContext jssc;

	private static final Pattern SPACE = Pattern.compile(" ");

	/**
	 * empty constructor
	 */
	private SparkStreamingJSonJob() {
	}

	public SparkStreamingJSonJob(String[] strings) {
		this.args = strings;
	}

	/**
	 * To set the log levels
	 */
	private static void setLogLevels() {
		boolean log4jInitialized = Logger.getRootLogger().getAllAppenders().hasMoreElements();
		if (!log4jInitialized) {
			// We first log something to initialize Spark's default logging,
			// then we override the
			// logging level.
			Logger.getLogger(SparkStreamingJSonJob.class).info("Setting log level to [WARN] for streaming job"
					+ " To override add a custom log4j.properties to the classpath.");
			Logger.getRootLogger().setLevel(Level.WARN);
		}
	}

	/**
	 * main method implementation
	 */
	public static void main(String[] args) throws Exception {

		System.setProperty("hadoop.home.dir", "c:/winutils/");

		if (args.length < 4) {
			System.err.println("Usage: StreamingJob <zkQuorum> <group> <topics> <numThreads>");
			System.exit(1);
		}

		setLogLevels();

		SparkConf sparkConf = new SparkConf().setAppName("StreamingJob").setMaster("local[16]");
		// Create the context with 1 seconds batch size
		jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		int numThreads = Integer.parseInt(args[3]);
		Map<String, Integer> topicMap = new HashMap<>();
		String[] topics = args[2].split(",");
		for (String topic : topics) {
			topicMap.put(topic, numThreads);
		}

		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils.createStream(jssc, args[0], args[1],
				topicMap);

		// Instantiate the accumulator
		messCounter = jssc.sparkContext().accumulator(0, "messCount");
		count = 0;

		messages.foreachRDD(new VoidFunction<JavaPairRDD<String, String>>() {

			@Override
			public void call(JavaPairRDD<String, String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<Tuple2<String, String>>() {
					// File file = new File("C:/Temp/" + rdd.id()+".txt");
					// BufferedWriter bufferedWriter = new BufferedWriter(new
					// FileWriter(file.getAbsolutePath()));

					@Override
					public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
						// increment accumulator value for every message call
						messCounter.add(1);
						count++;
						JSONObject json = new JSONObject(stringStringTuple2._2);
						System.out.println(
								"Time for streaming (ms): " + (System.currentTimeMillis() - json.getLong("Time")));
						if (count < 5000) {
							int num = json.getInt("message_no");
							// bufferedWriter.write("Message number: " + json.getInt("message_no") + ",,, Time :" + (System.currentTimeMillis() - json.getLong("Time")));
							// bufferedWriter.newLine();
							messTime.put(json.getInt("message_no"),
									(System.currentTimeMillis() - json.getLong("Time")));
						}
					}
				});

			}

		});

//		messages.dstream().saveAsTextFiles("C:/Temp/output", "txt");

		jssc.start();
	}

	/**
	 * return the value of accumulator called in the driver program
	 */
	public static Integer getAccumulator() {
		return messCounter.value();
	}

	public static Map<Integer, Long> getMessTime() {
		return messTime;
	}

	@Override
	public void run() {
		try {
			SparkStreamingJSonJob.main(args);
		} catch (Exception e) {
			e.printStackTrace();

		}
	}
}
