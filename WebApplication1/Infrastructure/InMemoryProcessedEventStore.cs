using System.Collections.Concurrent;
using WebApplication1.Domain;
using WebApplication1.Domain.Interfaces;

namespace WebApplication1.Infrastructure;

public sealed class InMemoryProcessedEventStore : IProcessedEventStore
{
    private readonly ConcurrentQueue<EventEnvelope> _events = new();

    public void Add(EventEnvelope envelope)
    {
        _events.Enqueue(envelope);
        // Keep memory bounded in a naive way (optional): cap at 10_000 events
        while (_events.Count > 10_000 && _events.TryDequeue(out _))
        {
        }
    }

    public IReadOnlyList<EventEnvelope> GetAll(int? limit = null)
    {
        var snapshot = _events.ToArray();
        if (limit is > 0)
        {
            return snapshot.Take((int)limit).ToArray();
        }
        return snapshot;
    }
}