package guru.learningjournal.kafka.examples;

class AppConfigs {

    final static String applicationID = "CountingSessionApp";
    final static String bootstrapServers = "localhost:9092";
    final static String topicName = "user-clicks-topic";
    final static String stateStoreName = "tmp/state-store";
}
