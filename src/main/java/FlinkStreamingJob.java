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


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingAlignedProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.codehaus.jettison.json.JSONObject;
import scala.Tuple2;
import scala.collection.parallel.ParIterableLike;

import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
/**
 * Created by Likith on 18-04-2017.
 */

/**
 * Read Strings from Kafka and print them to standard out.
 * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
 *
 * Please pass the following arguments to run the example:
 * 	--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 *
 */
public class FlinkStreamingJob {
	static int count =0;
	static Timer timer=new Timer();

	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", "c:/winutils/");
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if(parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
					"--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(1000); // create a checkpoint every 1 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		
		DataStream<String> messageStream = env
				.addSource(new FlinkKafkaConsumer010<>(
						parameterTool.getRequired("topic"),
						new SimpleStringSchema(),
						parameterTool.getProperties()));

//		SingleOutputStreamOperator<Tuple1> sum = messageStream.map(line->new Tuple1(1)).keyBy(0).window(TumblingAlignedProcessingTimeWindows.of(Time.seconds(1))).sum(0);
		
	
		
//		System.out.println("Records-per-second-"+sum.getTransformation().getBufferTimeout());
		
		messageStream.map(new MapFunction<String, String>() {
			
			@Override
			public String map(String value) throws Exception {
//				String[] firstVal=value.split(",");
//				String[] split = firstVal[0].split(":");
//				int messageValue = Integer.parseInt(split[2]);
//				
//				String[] split2 = firstVal[1].split(":");
//				String messageTime =split2[1];
//				
				
				
//				if(seconds==Calendar.getInstance().getTime().getSeconds()){
//					count++;
//				}
//				else{
//					System.out.println("Flink Streaming Throughput (Msgs/sec) :"+count );
//					count=0;
//					seconds=Calendar.getInstance().getTime().getSeconds();
//					}
				
//				System.out.println("Flink Streaming Throughput (Msgs/sec) :"+value);
				
				JSONObject json = new JSONObject(value);
				System.out.println("Time for streaming (ms): " +(System.currentTimeMillis() - json.getLong("Time")));
				 
				
				return value;
			}
		});
		

		// write kafka stream to standard out
//		messageStream.windowAll(, Time.of(1, TimeUnit.SECONDS));
//		AllWindowedStream<String,TimeWindow> timeWindowAll = messageStream.timeWindowAll(Time.seconds(1));
//		messageStream.print();
		env.execute();
		
	}
	
	public static int countMessages(){
		int val = count ++;
		
		
		return val ;
		
	}
}