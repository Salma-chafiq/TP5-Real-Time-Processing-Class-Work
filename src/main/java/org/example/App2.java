package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class App2 {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("application.id" , "total-amount-client");
    props.put("bootstrap.servers","localhost:9092");
    props.put("default.key.serde", Serdes.String().getClass());
    props.put("default.value.serde", Serdes.String().getClass());

    //Construire un Stream
    StreamsBuilder builder = new StreamsBuilder();
    KStream<String ,String> ordersStream = builder.stream("orders");//la source lire les message publiee dans le topic
    //nom,total
    KStream<String ,String> filteredOrders = ordersStream.filter((key,val)->{
      double amount = Double.parseDouble(val.split(",")[1]);
      return amount>100;

  });
  //Calculer par client
    KGroupedStream<String,String > groupedOrders=filteredOrders.groupBy((key,val)-> val.split(",")[0]);
    KTable<String,Double>totalByCustumer = groupedOrders.aggregate(()->0.0,(key , val, somme)->{//client,montant
      double amount = Double.parseDouble(val.split(",")[1]);
      //lopreration
      return somme+amount;
    }, Materialized.with(Serdes.String(),Serdes.Double()));//serialisation);
    totalByCustumer.toStream().to("customer-total");




    KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),props);
    kafkaStreams.start();


    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));


  }
}
