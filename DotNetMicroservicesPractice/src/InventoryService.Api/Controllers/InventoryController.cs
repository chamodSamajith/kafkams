using Microsoft.AspNetCore.Mvc;
using StackExchange.Redis;

namespace InventoryService.Api.Controllers;

[ApiController]
[Route("api/inventory")]
public class InventoryController : ControllerBase
{
    private readonly IConnectionMultiplexer _redis;

    public InventoryController(IConnectionMultiplexer redis)
    {
        _redis = redis;
    }

    [HttpGet("{productId}")]
    public async Task<IActionResult> GetStock(string productId)
    {
        var redisDb = _redis.GetDatabase();

        var stock = await redisDb.StringGetAsync($"stock:{productId}");

        if (stock.IsNullOrEmpty)
        {
            return Ok(new
            {
                ProductId = productId,
                Stock = 100,
                Message = "Default stock. No stock record found in Redis yet."
            });
        }

        return Ok(new
        {
            ProductId = productId,
            Stock = int.Parse(stock!)
        });
    }

    [HttpPost("{productId}/stock/{quantity}")]
    public async Task<IActionResult> SetStock(string productId, int quantity)
    {
        if (quantity < 0)
            return BadRequest("Quantity cannot be negative.");

        var redisDb = _redis.GetDatabase();

        await redisDb.StringSetAsync($"stock:{productId}", quantity);

        return Ok(new
        {
            ProductId = productId,
            Stock = quantity,
            Message = "Stock updated."
        });
    }
}