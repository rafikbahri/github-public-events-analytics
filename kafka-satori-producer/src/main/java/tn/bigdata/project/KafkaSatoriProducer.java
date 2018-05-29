package tn.bigdata.project;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.satori.rtm.*;
import com.satori.rtm.model.*;

public class KafkaSatoriProducer {
    static final String endpoint = "wss://open-data.api.satori.com";
    static final String appkey = "f1eACC7e0b5B5e8C44Cc648eBe9fac13";
    static final String channel = "github-events";
    static final String topicName = "GithubEventsTopic";


    public static void main(String[] args) throws Exception{




        // Creer une instance de proprietes pour acceder aux configurations du producteur
        Properties props = new Properties();

        // Assigner l'identifiant du serveur kafka
        props.put("bootstrap.servers", "localhost:9092");

        // Definir un acquittement pour les requetes du producteur
        props.put("acks", "all");

        // Si la requete echoue, le producteur peut reessayer automatiquemt
        props.put("retries", 0);

        // Specifier la taille du buffer size dans la config
        props.put("batch.size", 16384);

        // buffer.memory controle le montant total de memoire disponible au producteur pour le buffering
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        final Producer<String, String> producer = new KafkaProducer
                <String, String>(props);

        //Satori part
        final RtmClient client = new RtmClientBuilder(endpoint, appkey)
                .setListener(new RtmClientAdapter() {
                    @Override
                    public void onEnterConnected(RtmClient client) {
                        System.out.println("Connected to Satori RTM!");
                    }
                })
                .build();


        SubscriptionAdapter listener = new SubscriptionAdapter() {
            @Override
            public void onSubscriptionData(SubscriptionData data) {
                int key=0;
                for (AnyJson json : data.getMessages()) {
                    System.out.println("Got message: " + json.toString());
                    ProducerRecord<String,String> githubEvent=new ProducerRecord<String, String>(topicName,Integer.toString(key),json.toString());
                    producer.send(githubEvent);
                    System.out.println("Sent to topic!");
                }


            }
        };

        client.createSubscription(channel, SubscriptionMode.SIMPLE, listener);
        client.start();
    }
}