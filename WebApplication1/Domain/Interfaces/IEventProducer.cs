namespace WebApplication1.Domain.Interfaces;

public interface IEventProducer
{
    Task ProduceAsync(EventDto @event, CancellationToken cancellationToken);
}