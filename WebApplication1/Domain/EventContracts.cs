namespace WebApplication1.Domain;

public sealed record EventEnvelope
(
    Guid Id,
    DateTimeOffset ProcessedAt,
    EventDto Payload
);