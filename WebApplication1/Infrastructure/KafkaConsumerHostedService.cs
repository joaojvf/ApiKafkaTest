using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using WebApplication1.Domain;
using WebApplication1.Domain.Interfaces;

namespace WebApplication1.Infrastructure;

public sealed class KafkaConsumerHostedService : BackgroundService
{
    private readonly ILogger<KafkaConsumerHostedService> _logger;
    private readonly IProcessedEventStore _store;
    private readonly KafkaSettings _settings;

    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public KafkaConsumerHostedService(
        ILogger<KafkaConsumerHostedService> logger,
        IProcessedEventStore store,
        IOptions<KafkaSettings> options)
    {
        _logger = logger;
        _store = store;
        _settings = options.Value;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _settings.ConsumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason}", e.Reason))
            .Build();

        consumer.Subscribe(_settings.Topic);
        _logger.LogInformation("Kafka consumer subscribed to topic {Topic}", _settings.Topic);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var cr = consumer.Consume(stoppingToken);
                    if (cr is null) continue;

                    var dto = JsonSerializer.Deserialize<EventDto>(cr.Message.Value, JsonOptions);
                    if (dto is null)
                    {
                        _logger.LogWarning("Received null or invalid message");
                        continue;
                    }

                    var envelope = new EventEnvelope(
                        Id: Guid.NewGuid(),
                        ProcessedAt: DateTimeOffset.UtcNow,
                        Payload: dto);

                    _store.Add(envelope);
                    _logger.LogInformation("Processed event for user {UserId} type {Type}", dto.UserId, dto.Type);
                }
                catch (ConsumeException ce)
                {
                    _logger.LogError(ce, "Consume error: {Reason}", ce.Error.Reason);
                }
                catch (OperationCanceledException)
                {
                    // Shutdown
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error while consuming Kafka message");
                }
            }
        }
        finally
        {
            consumer.Close();
        }

        await Task.CompletedTask;
    }
}
