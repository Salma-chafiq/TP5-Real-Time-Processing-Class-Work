package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class App1 {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("application.id" , "tp5-kafka-streams");
    props.put("bootstrap.servers","localhost:9092");
    props.put("default.key.serde", Serdes.String().getClass());
    props.put("default.value.serde", Serdes.String().getClass());

    //Construire un Stream
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String ,String> sourceStream = builder.stream("input-topic");//la source lire les message publiee dans le topic
    KStream<String ,String>upperCaseStream = sourceStream.mapValues(val ->{
      System.out.println(val);
      return val.toUpperCase();
      });
    //Transformer a une autre stream
    upperCaseStream.to("output-topic");//Ecrire dans output stream

    KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
    kafkaStreams.start();


    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));



  }
}
