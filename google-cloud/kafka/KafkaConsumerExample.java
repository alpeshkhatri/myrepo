import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option.Builder;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.ParseException;

import java.util.Collections;
import java.util.Properties;
import java.time.Duration;


public class KafkaConsumerExample {

  public static void main(String[] args) {
        CommandLine commandLine;
        Options options = new Options();
        CommandLineParser parser = new DefaultParser();
        Option option_bootstrap = Option.builder("bootstrap")
            .required(true)
            .desc("bootstrap")
            .longOpt("bootstrap")
	    .hasArg(true)
            .build();
        options.addOption(option_bootstrap);

        Option option_topic = Option.builder("topic")
            .required(true)
            .desc("topic")
            .longOpt("topic")
	    .hasArg(true)
            .build();
        options.addOption(option_topic);

        Option option_partition = Option.builder("partition")
            .required(true)
            .desc("partition")
            .longOpt("partition")
	    .hasArg(true)
            .build();
        options.addOption(option_partition);

        Option option_offset = Option.builder("offset")
            .required(true)
            .desc("offset")
            .longOpt("offset")
	    .hasArg(true)
            .build();
        options.addOption(option_offset);

        try
        {
            commandLine = parser.parse(options, args);

            System.out.print("Option bootstrap is present.  The value is: " + commandLine.getOptionValue("bootstrap"));
            System.out.print("Option topic     is present.  The value is: " + commandLine.getOptionValue("topic"));
            System.out.print("Option partition is present.  The value is: " + commandLine.getOptionValue("partition"));
            System.out.print("Option offset    is present.  The value is: " + commandLine.getOptionValue("offset"));

    Properties props = new Properties();
    props.put("bootstrap.servers", commandLine.getOptionValue("bootstrap"));
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    // assign cannot take group id
    props.put("group.id", "test-group");
    //
    /*
     * String topic = "foo";
     * TopicPartition partition0 = new TopicPartition(topic, 0);
     * TopicPartition partition1 = new TopicPartition(topic, 1);
     * consumer.assign(Arrays.asList(partition0, partition1));
     */

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(commandLine.getOptionValue("topic")));
    //System.out.println("Connected to Kakfa as KafkaConsumer");

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	//System.out.println("in consumer.pool loop");
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
      }
    } catch (Exception e) {
	    e.printStackTrace() ;
    }
  }    catch (ParseException exception)
        {
            System.out.print("Parse error: ");
            System.out.println(exception.getMessage());
        }

  }
}
