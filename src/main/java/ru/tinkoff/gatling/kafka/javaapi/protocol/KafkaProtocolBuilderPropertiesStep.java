package ru.tinkoff.gatling.kafka.javaapi.protocol;

import java.util.Map;
import static scala.jdk.javaapi.CollectionConverters.asScala;

public class KafkaProtocolBuilderPropertiesStep {

    private final String topic;
    private Map<String, Object> props;

    public KafkaProtocolBuilderPropertiesStep(String topic, Map<String, Object> props){
        this.topic = topic;
        this.props = props;
    }

    public KafkaProtocolBuilder properties(Map<String, Object> props) {
        this.props = props;
        scala.collection.immutable.Map<String, Object> scalaMap = scala.collection.immutable.Map.from(asScala(this.props));
        return new KafkaProtocolBuilder(
                ru.tinkoff.gatling.kafka.protocol.KafkaProtocolBuilderPropertiesStep.apply(this.topic, scalaMap)
                        .properties(scalaMap)
        );
    }


}
