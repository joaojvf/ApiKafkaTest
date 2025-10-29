namespace WebApplication1.Domain.Interfaces;

public interface IProcessedEventStore
{
    void Add(EventEnvelope envelope);
    IReadOnlyList<EventEnvelope> GetAll(int? limit = null);
}