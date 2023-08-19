using Microsoft.EntityFrameworkCore;

namespace OwnDataSpaces.Postgres.Api;

public class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options)
    {
    }

    public DbSet<Order> Orders => Set<Order>();
    public DbSet<OrderItem> Items => Set<OrderItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.HasDefaultSchema("Smart");
        modelBuilder.Entity<Order>().HasIndex(x => x.Code).IsUnique();
        modelBuilder.Entity<Order>().HasIndex(x => new { x.Part1, x.Part2 }).IsUnique();
    }
}