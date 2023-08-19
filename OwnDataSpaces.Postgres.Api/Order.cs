using System.ComponentModel.DataAnnotations.Schema;

namespace OwnDataSpaces.Postgres.Api;

[Table("Orders", Schema = "Smart")]
public class Order
{
    public Guid Id { get; set; }
    public string Code { get; set; }
    public string Part1 { get; set; }
    public string Part2 { get; set; }
    public string Comments { get; set; }
    public List<OrderItem> Items { get; set; }
}

[Table("OrderItems", Schema = "Smart")]
public class OrderItem
{
    public Guid Id { get; set; }
    public int Quantity { get; set; }

    public Guid OrderId { get; set; }
}

[Table("Items", Schema = "Smart")]
public class ItemDescriptor
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public string Description { get; set; }
}