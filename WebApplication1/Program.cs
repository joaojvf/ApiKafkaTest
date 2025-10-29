using Microsoft.Extensions.Options;
using WebApplication1.Domain;
using WebApplication1.Domain.Interfaces;
using WebApplication1.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// Bind Kafka settings
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));

// DI registrations
builder.Services.AddSingleton<IProcessedEventStore, InMemoryProcessedEventStore>();
builder.Services.AddSingleton<IEventProducer, KafkaEventProducer>();
builder.Services.AddHostedService<KafkaConsumerHostedService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

// Minimal API endpoints
app.MapPost("/events", async (EventDto dto, IEventProducer producer, ILoggerFactory loggerFactory, CancellationToken ct) =>
    {
        var logger = loggerFactory.CreateLogger("POST /events");
        try
        {
            await producer.ProduceAsync(dto, ct);
            return Results.Accepted("/events");
        }
        catch (ArgumentException ex)
        {
            logger.LogWarning(ex, "Bad request while producing event");
            return Results.BadRequest(new { error = ex.Message });
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to produce event");
            return Results.Problem("Failed to accept event");
        }
    })
    .WithName("PostEvent")
    .WithSummary("Accept a user activity event and publish to Kafka topic.")
    .WithOpenApi();

app.MapGet("/events", (int? limit, IProcessedEventStore store) =>
    {
        var events = store.GetAll(limit);
        return Results.Ok(events);
    })
    .WithName("GetProcessedEvents")
    .WithSummary("Get list of processed events from in-memory store.")
    .WithOpenApi();

app.Run();