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
            .desc("The bootstrap option")
            .longOpt("bootstrap")
	    .hasArg(true)
            .build();
        options.addOption(option_bootstrap);
        Option option_topic = Option.builder("topic")
            .required(true)
            .desc("The r option")
            .longOpt("topic")
	    .hasArg(true)
            .build();
        options.addOption(option_topic);

        try
        {
            commandLine = parser.parse(options, args);

            if (commandLine.hasOption("bootstrap"))
            {
                System.out.print("Option bootstrap is present.  The value is: ");
                System.out.println(commandLine.getOptionValue("bootstrap"));
            }

            if (commandLine.hasOption("topic"))
            {
                System.out.print("Option topic is present.  The value is: ");
                System.out.println(commandLine.getOptionValue("topic"));
            }

            {
                String[] remainder = commandLine.getArgs();
                System.out.print("Remaining arguments: ");
                for (String argument : remainder)
                {
                    System.out.print(argument);
                    System.out.print(" ");
                }

                System.out.println();
            }

    Properties props = new Properties();
    // props.put("bootstrap.servers", "broker:9092");
    props.put("bootstrap.servers", commandLine.getOptionValue("bootstrap"));
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("group.id", "test-group");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    //consumer.subscribe(Collections.singletonList("quickstart"));
    consumer.subscribe(Collections.singletonList(commandLine.getOptionValue("topic")));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
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
