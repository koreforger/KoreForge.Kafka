using System.Threading;

namespace KF.Kafka.Producer.Buffer;

internal sealed class TopicBufferState
{
    private long _buffered;

    public TopicBufferState(int capacity)
    {
        Capacity = capacity;
    }

    public int Capacity { get; }

    public long Buffered => Interlocked.Read(ref _buffered);

    public void Increment()
    {
        Interlocked.Increment(ref _buffered);
    }

    public void Decrement()
    {
        var value = Interlocked.Decrement(ref _buffered);
        if (value < 0)
        {
            Interlocked.Exchange(ref _buffered, 0);
        }
    }
}
