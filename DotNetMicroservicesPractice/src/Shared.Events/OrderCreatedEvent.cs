namespace Shared.Events;

public class OrderCreatedEvent
{
    public Guid EventId { get; set; } = Guid.NewGuid();

    public string EventType { get; set; } = "OrderCreated";

    public Guid OrderId { get; set; }

    public string CustomerId { get; set; } = string.Empty;

    public string ProductId { get; set; } = string.Empty;

    public int Quantity { get; set; }

    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}