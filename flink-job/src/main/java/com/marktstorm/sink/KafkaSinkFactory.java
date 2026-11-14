    package com.marktstorm.sink;

    import com.fasterxml.jackson.databind.ObjectMapper;

    import org.apache.flink.connector.kafka.sink.KafkaSink;
    import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
    import org.apache.flink.connector.base.DeliveryGuarantee;


    public class KafkaSinkFactory {
        public static <T> KafkaSink<T> create(String topic){
            ObjectMapper mapper = new ObjectMapper();

            return KafkaSink.<T>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(
                    KafkaRecordSerializationSchema.<T>builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(
                            element ->{
                                try{
                                    return mapper.writeValueAsBytes(element);
                                }catch(Exception e){
                                    throw new RuntimeException(e);
                                }
                            }
                        )
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        }
    }
