using System;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Confluent.Kafka.SyncOverAsync;
using br.com.foo.kafka.avro;

namespace confluent_kafka_producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Action<DeliveryReport<Null, Cliente>> handler =
                    r => Console.WriteLine(!r.Error.IsError ? $"Delivered message to {r.TopicPartitionOffset}" : $"Delivery Error: {r.Error.Reason}");


            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "https://schema-registry:8181",
                RequestTimeoutMs = 5000,
                MaxCachedSchemas = 10,
                ValueSubjectNameStrategy = SubjectNameStrategy.TopicRecord,
                SslCaLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/CAroot.pem",
                SslCertificateLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/schema-registry.keystore.jks",
                SslCertificatePassword = "datahub"
            };

            var config = new ProducerConfig
            {
                BootstrapServers = "kafka-ssl:9092",
                ClientId = "my_producer",
                SecurityProtocol = SecurityProtocol.Ssl,
                SslCaLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/CAroot.pem",
                SslKeystoreLocation = "C:/Users/raul/workspace/kafka-ssl-compose/secrets/producer.keystore.jks",
                SslKeystorePassword = "datahub",
                Debug = "security",
                CompressionType = CompressionType.Gzip,
                EnableIdempotence = true,
                BatchNumMessages = 16000,
                LingerMs = 5,
                Acks = Acks.All,
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))

            using (var producer = new ProducerBuilder<Null, Cliente>(config)
            .SetValueSerializer(new AvroSerializer<Cliente>(schemaRegistry, new AvroSerializerConfig { AutoRegisterSchemas = false }).AsSyncOverAsync())
            .Build())
            {
                for (int i = 0; i < 5; ++i)
                {
                    Cliente cliente = new Cliente { clienteId = i + "", name = "John" };
                    producer.Produce("client", new Message<Null, Cliente> { Value = cliente }, handler);
                    Console.WriteLine("Sending message");


                    producer.Flush(TimeSpan.FromSeconds(10));
                }
            }
        }
    }    
}
