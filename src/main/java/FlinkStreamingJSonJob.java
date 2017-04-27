
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by Pai on 19-04-2017.
 */
public class FlinkStreamingJSonJob {

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
        env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

//        DataStream<String> messageStream = env
//                .addSource(new FlinkKafkaConsumer010<>(
//                        parameterTool.getRequired("topic"),
//                        new JSONKeyValueDeserializationSchema(false),
//                        parameterTool.getProperties()));

        // write kafka stream to standard out.
//        messageStream.print();

        env.execute();
    }
}
