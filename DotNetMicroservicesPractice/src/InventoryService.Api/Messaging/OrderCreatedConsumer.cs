using System.Text.Json;
using Confluent.Kafka;
using Shared.Events;
using StackExchange.Redis;

namespace InventoryService.Api.Messaging;

public class OrderCreatedConsumer : BackgroundService
{
    private readonly IConfiguration _configuration;
    private readonly IConnectionMultiplexer _redis;

    public OrderCreatedConsumer(
        IConfiguration configuration,
        IConnectionMultiplexer redis)
    {
        _configuration = configuration;
        _redis = redis;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _configuration["Kafka:BootstrapServers"],
            GroupId = _configuration["Kafka:ConsumerGroup"],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();

        var topic = _configuration["Kafka:OrderCreatedTopic"];

        if (string.IsNullOrWhiteSpace(topic))
        {
            Console.WriteLine("Kafka topic is not configured.");
            return;
        }

        consumer.Subscribe(topic);

        Console.WriteLine($"InventoryService is listening to Kafka topic: {topic}");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(stoppingToken);

                var orderEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

                if (orderEvent == null)
                {
                    Console.WriteLine("Invalid OrderCreated event received.");
                    continue;
                }

                await ProcessOrderCreatedEvent(orderEvent);

                consumer.Commit(result);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Kafka consumer stopping...");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error consuming Kafka message: {ex.Message}");
            }
        }

        consumer.Close();
    }

    private async Task ProcessOrderCreatedEvent(OrderCreatedEvent orderEvent)
    {
        var redisDb = _redis.GetDatabase();

        var productStockKey = $"stock:{orderEvent.ProductId}";

        var currentStockValue = await redisDb.StringGetAsync(productStockKey);

        int currentStock;

        if (currentStockValue.IsNullOrEmpty)
            currentStock = 100;
        else
            currentStock = int.Parse(currentStockValue!);

        var newStock = currentStock - orderEvent.Quantity;

        if (newStock < 0)
        {
            Console.WriteLine($"Not enough stock for product {orderEvent.ProductId}");
            return;
        }

        await redisDb.StringSetAsync(productStockKey, newStock);

        await redisDb.StringSetAsync(
            $"processed-event:{orderEvent.EventId}",
            DateTime.UtcNow.ToString("O"),
            TimeSpan.FromDays(1)
        );

        Console.WriteLine("OrderCreated event processed.");
        Console.WriteLine($"OrderId: {orderEvent.OrderId}");
        Console.WriteLine($"ProductId: {orderEvent.ProductId}");
        Console.WriteLine($"Quantity: {orderEvent.Quantity}");
        Console.WriteLine($"Remaining stock: {newStock}");
    }
}