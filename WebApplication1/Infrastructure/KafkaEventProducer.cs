using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using WebApplication1.Domain;
using WebApplication1.Domain.Interfaces;

namespace WebApplication1.Infrastructure;

public sealed class KafkaEventProducer : IEventProducer, IAsyncDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaSettings _settings;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public KafkaEventProducer(IOptions<KafkaSettings> options)
    {
        _settings = options.Value;
        var config = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            EnableIdempotence = true
        };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task ProduceAsync(EventDto @event, CancellationToken cancellationToken)
    {
        // Basic validation before producing
        Validate(@event);

        var payload = JsonSerializer.Serialize(@event with
        {
            Timestamp = @event.Timestamp ?? DateTimeOffset.UtcNow
        }, JsonOptions);

        var message = new Message<string, string>
        {
            Key = @event.UserId,
            Value = payload
        };

        await _producer.ProduceAsync(_settings.Topic, message, cancellationToken).ConfigureAwait(false);
    }

    private static void Validate(EventDto dto)
    {
        if (string.IsNullOrWhiteSpace(dto.UserId))
            throw new ArgumentException("userId is required");
        if (string.IsNullOrWhiteSpace(dto.Type))
            throw new ArgumentException("type is required");
        if (!Enum.TryParse<UserActivityType>(dto.Type, ignoreCase: true, out _))
            throw new ArgumentException("type must be one of: login, logout, purchase");
    }

    public async ValueTask DisposeAsync()
    {
        _producer.Flush(TimeSpan.FromSeconds(3));
        _producer.Dispose();
        await Task.CompletedTask;
    }
}
