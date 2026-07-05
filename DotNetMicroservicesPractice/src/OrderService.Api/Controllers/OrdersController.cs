using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using OrderService.Api.Messaging;
using OrderService.Api.Models;
using Shared.Events;
using StackExchange.Redis;

namespace OrderService.Api.Controllers;

[ApiController]
[Route("api/orders")]
public class OrdersController : ControllerBase
{
    private readonly KafkaProducer _kafkaProducer;
    private readonly IConfiguration _configuration;
    private readonly IConnectionMultiplexer _redis;

    public OrdersController(
        KafkaProducer kafkaProducer,
        IConfiguration configuration,
        IConnectionMultiplexer redis)
    {
        _kafkaProducer = kafkaProducer;
        _configuration = configuration;
        _redis = redis;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder(CreateOrderRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.CustomerId))
            return BadRequest("CustomerId is required.");

        if (string.IsNullOrWhiteSpace(request.ProductId))
            return BadRequest("ProductId is required.");

        if (request.Quantity <= 0)
            return BadRequest("Quantity must be greater than 0.");

        var orderId = Guid.NewGuid();

        var orderCreatedEvent = new OrderCreatedEvent
        {
            OrderId = orderId,
            CustomerId = request.CustomerId,
            ProductId = request.ProductId,
            Quantity = request.Quantity
        };

        var redisDb = _redis.GetDatabase();

        await redisDb.StringSetAsync(
            $"order:{orderId}",
            JsonSerializer.Serialize(orderCreatedEvent),
            TimeSpan.FromMinutes(30)
        );

        var topic = _configuration["Kafka:OrderCreatedTopic"];

        if (string.IsNullOrWhiteSpace(topic))
            return StatusCode(500, "Kafka topic is not configured.");

        await _kafkaProducer.PublishAsync(topic, orderCreatedEvent);

        return Ok(new
        {
            Message = "Order created and OrderCreated event published.",
            OrderId = orderId,
            EventId = orderCreatedEvent.EventId
        });
    }

    [HttpGet("{orderId}")]
    public async Task<IActionResult> GetOrder(Guid orderId)
    {
        var redisDb = _redis.GetDatabase();

        var order = await redisDb.StringGetAsync($"order:{orderId}");

        if (order.IsNullOrEmpty)
            return NotFound("Order not found in Redis cache.");

        return Ok(JsonSerializer.Deserialize<object>(order!));
    }
}