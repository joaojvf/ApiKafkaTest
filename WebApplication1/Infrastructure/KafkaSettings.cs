namespace WebApplication1.Infrastructure;

public sealed class KafkaSettings
{
    public string BootstrapServers { get; set; } = "localhost:9092";
    public string Topic { get; set; } = "user-activity";
    public string ConsumerGroupId { get; set; } = "webapp1-consumer";
    public bool EnableAutoCreateTopics { get; set; } = true; // for local dev clusters
}
