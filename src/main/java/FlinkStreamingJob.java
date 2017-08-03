
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.codehaus.jettison.json.JSONObject;

/**
 * @author likith,pai
 */
/**
 * Read Strings from Kafka and print them to standard out. Note: On a cluster,
 * DataStream.print() will print to the TaskManager's .out file!
 *
 * Please pass the following arguments to run the example: --topic test
 * --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181
 * --group.id myconsumer
 *
 */
public class FlinkStreamingJob implements RichFunction, Runnable {

	private static String[] args = {};

	public FlinkStreamingJob(String[] strings) {
		args = strings;
	}

	private static LongCounter messagesCounter;

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "c:/winutils/");
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> "
					+ "--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(1000); // create a checkpoint every 1 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make
																// parameters
																// available in
																// the web
																// interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer010<>(parameterTool.getRequired("topic"),
				new SimpleStringSchema(), parameterTool.getProperties()));

		messagesCounter = new LongCounter();
		messageStream.map(new MapFunction<String, String>() {

			@Override
			public String map(String value) throws Exception {
				messagesCounter.add(1);
				JSONObject json = new JSONObject(value);
				System.out.println("Time for streaming (ms): " + (System.currentTimeMillis() - json.getLong("Time")));

				return value;
			}
		});

		// write kafka stream to standard out
		messageStream.print();
		env.execute();

	}

	/**
	 * 
	 * @return
	 */
	public static LongCounter getmessagesCounter() {
		return messagesCounter;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		getRuntimeContext().addAccumulator("messagesCounter", messagesCounter);

	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public RuntimeContext getRuntimeContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setRuntimeContext(RuntimeContext t) {
		// TODO Auto-generated method stub

	}

	/**
	 * Thread implementation calling flink main job
	 */
	@Override
	public void run() {
		try {
			FlinkStreamingJob.main(args);
		} catch (Exception e) {
			e.printStackTrace();

		}
	}

}