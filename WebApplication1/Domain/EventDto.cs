using System.Text.Json;

namespace WebApplication1.Domain;

public sealed record EventDto
{
    public required string UserId { get; init; }
    public required string Type { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public JsonElement Data { get; init; }
}