using System.Net.Http.Json;
using FluentAssertions;
using OwnDataSpaces.SqlServer.Api;
using OwnDataSpaces.SqlServer.Tests.Fixtures;
using Xunit.Abstractions;

namespace OwnDataSpaces.SqlServer.Tests;

[Collection(nameof(IntegrationTestFixtureCollection))]
public class RunningMultipleTestsInParallel
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly OwnSpace _space;

    public RunningMultipleTestsInParallel(ITestOutputHelper testOutputHelper, IntegrationTestFixture fixture)
    {
        _testOutputHelper = testOutputHelper;
        _space = fixture.Api.CreateOwnSpace();
        fixture.Api.OutputHelper = testOutputHelper;
    }

    [Fact]
    public async Task Test_1()
    {
        var client = _space.GetClient();

        var count = Random.Shared.Next(5, 20);
        _testOutputHelper.WriteLine("Expecting: {0}", count);
        foreach (var i in Enumerable.Range(1, count))
        {
            var createResult =
                await client.PostAsJsonAsync($"?code=code_{i}", new { });
            createResult.EnsureSuccessStatusCode();
        }

        var result = await client.GetFromJsonAsync<List<Order>>("/");

        result.Should().HaveCount(count);
    }

    [Fact]
    public async Task Test_2()
    {
        var client = _space.GetClient();

        var count = Random.Shared.Next(5, 20);
        _testOutputHelper.WriteLine("Expecting: {0}", count);
        foreach (var i in Enumerable.Range(1, count))
        {
            var createResult =
                await client.PostAsJsonAsync($"?code=code_{i}", new { });
            createResult.EnsureSuccessStatusCode();
        }

        var result = await client.GetFromJsonAsync<List<Order>>("/");

        result.Should().HaveCount(count);
    }
}