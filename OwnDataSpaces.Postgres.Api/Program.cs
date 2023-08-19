using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OwnDataSpaces.Postgres.Api;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(opt =>
{
    opt.UseNpgsql(builder.Configuration.GetConnectionString("Default"));
    opt.EnableDetailedErrors();
    opt.EnableSensitiveDataLogging();
});

var app = builder.Build();

app.MapGet("/", async (AppDbContext dbContext) =>
{
    var orders = await dbContext.Orders.ToListAsync();
    return Results.Ok(orders);
});

app.MapPost("/", async ([FromQuery] string code, AppDbContext dbContext) =>
{
    var order = new Order
    {
        Id = Guid.NewGuid(),
        Comments = Guid.NewGuid().ToString(),
        Items = new List<OrderItem>(),
        Code = code,
        Part1 = Guid.NewGuid().ToString(),
        Part2 = Guid.NewGuid().ToString()
    };
    await dbContext.Orders.AddAsync(order);
    await dbContext.SaveChangesAsync();
    return Results.Ok(order);
});

app.Run();

namespace OwnDataSpaces.Postgres.Api
{
    public partial class Program
    {
    }
}