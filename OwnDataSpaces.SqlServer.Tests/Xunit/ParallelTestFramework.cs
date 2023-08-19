using System.Reflection;
using Xunit.Abstractions;
using Xunit.Sdk;

[assembly: TestFramework("OwnDataSpaces.SqlServer.Tests.Xunit.ParallelTestFramework", "OwnDataSpaces.SqlServer.Tests")]

namespace OwnDataSpaces.SqlServer.Tests.Xunit;

[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class DisableParallelizationAttribute : Attribute
{
}

public class ParallelTestFramework : XunitTestFramework
{
    public ParallelTestFramework(IMessageSink messageSink) : base(messageSink)
    {
    }

    protected override ITestFrameworkExecutor CreateExecutor(AssemblyName assemblyName)
        => new ParallelTestFrameworkExecutor(assemblyName, SourceInformationProvider, DiagnosticMessageSink);
}

public class ParallelTestFrameworkExecutor : XunitTestFrameworkExecutor
{
    public ParallelTestFrameworkExecutor(AssemblyName assemblyName,
        ISourceInformationProvider sourceInformationProvider, IMessageSink diagnosticMessageSink) : base(assemblyName,
        sourceInformationProvider, diagnosticMessageSink)
    {
    }

    protected override async void RunTestCases(IEnumerable<IXunitTestCase> testCases, IMessageSink executionMessageSink,
        ITestFrameworkExecutionOptions executionOptions)
    {
        using var assemblyRunner = new ParallelTestAssemblyRunner(TestAssembly, testCases, DiagnosticMessageSink,
            executionMessageSink, executionOptions);
        await assemblyRunner.RunAsync().ConfigureAwait(false);
    }
}

public class ParallelTestAssemblyRunner : XunitTestAssemblyRunner
{
    public ParallelTestAssemblyRunner(ITestAssembly testAssembly, IEnumerable<IXunitTestCase> testCases,
        IMessageSink diagnosticMessageSink, IMessageSink executionMessageSink,
        ITestFrameworkExecutionOptions executionOptions)
        : base(testAssembly, testCases, diagnosticMessageSink, executionMessageSink, executionOptions)
    {
    }

    protected override Task<RunSummary> RunTestCollectionAsync(IMessageBus messageBus, ITestCollection testCollection,
        IEnumerable<IXunitTestCase> testCases, CancellationTokenSource cancellationTokenSource)
        => new ParallelTestCollectionRunner(testCollection, testCases, DiagnosticMessageSink, messageBus,
            TestCaseOrderer, new ExceptionAggregator(Aggregator), cancellationTokenSource).RunAsync();
}

public class ParallelTestCollectionRunner : XunitTestCollectionRunner
{
    public ParallelTestCollectionRunner(ITestCollection testCollection, IEnumerable<IXunitTestCase> testCases,
        IMessageSink diagnosticMessageSink, IMessageBus messageBus, ITestCaseOrderer testCaseOrderer,
        ExceptionAggregator aggregator, CancellationTokenSource cancellationTokenSource)
        : base(testCollection, testCases, diagnosticMessageSink, messageBus, testCaseOrderer, aggregator,
            cancellationTokenSource)
    {
    }

    protected override async Task<RunSummary> RunTestClassesAsync()
    {
        var summary = new RunSummary();

        var chunks = TestCases
            .GroupBy(tc => tc.TestMethod.TestClass, TestClassComparer.Instance)
            .Chunk(50);
        
        foreach (var chunk in chunks)
        {
            var tasks = chunk.Select(async testCasesByClass =>
            {
                var runSummary = await RunTestClassAsync(testCasesByClass.Key,
                    (IReflectionTypeInfo)testCasesByClass.Key.Class,
                    testCasesByClass);
                summary.Aggregate(runSummary);
            });

            await Task.WhenAll(tasks);
        }

        return summary;
    }

    protected override Task<RunSummary> RunTestClassAsync(ITestClass testClass, IReflectionTypeInfo @class,
        IEnumerable<IXunitTestCase> testCases)
    {
        return new ParallelTestClassRunner(testClass, @class, testCases, DiagnosticMessageSink, MessageBus,
            TestCaseOrderer,
            new ExceptionAggregator(Aggregator), CancellationTokenSource, CollectionFixtureMappings).RunAsync();
    }
}

public class ParallelTestClassRunner : XunitTestClassRunner
{
    public ParallelTestClassRunner(ITestClass testClass, IReflectionTypeInfo @class,
        IEnumerable<IXunitTestCase> testCases, IMessageSink diagnosticMessageSink, IMessageBus messageBus,
        ITestCaseOrderer testCaseOrderer, ExceptionAggregator aggregator,
        CancellationTokenSource cancellationTokenSource, IDictionary<Type, object> collectionFixtureMappings)
        : base(testClass, @class, testCases, diagnosticMessageSink, messageBus, testCaseOrderer, aggregator,
            cancellationTokenSource, collectionFixtureMappings)
    {
    }

    // This method has been slightly modified from the original implementation to run tests in parallel
    // https://github.com/xunit/xunit/blob/2.4.2/src/xunit.execution/Sdk/Frameworks/Runners/TestClassRunner.cs#L194-L219
    protected override async Task<RunSummary> RunTestMethodsAsync()
    {
        var disableParallelization = TestClass.Class.GetCustomAttributes(typeof(DisableParallelizationAttribute)).Any();

        if (disableParallelization)
            return await base.RunTestMethodsAsync().ConfigureAwait(false);

        var summary = new RunSummary();
        IEnumerable<IXunitTestCase> orderedTestCases;
        try
        {
            orderedTestCases = TestCaseOrderer.OrderTestCases(TestCases);
        }
        catch (Exception ex)
        {
            var innerEx = Unwrap(ex);
            DiagnosticMessageSink.OnMessage(new DiagnosticMessage(
                $"Test case orderer '{TestCaseOrderer.GetType().FullName}' threw '{innerEx.GetType().FullName}' during ordering: {innerEx.Message}{Environment.NewLine}{innerEx.StackTrace}"));
            orderedTestCases = TestCases.ToList();
        }

        var constructorArguments = CreateTestClassConstructorArguments();
        var methodGroups = orderedTestCases.GroupBy(tc => tc.TestMethod, TestMethodComparer.Instance);
        var methodTasks = methodGroups.Select(m =>
            RunTestMethodAsync(m.Key, (IReflectionMethodInfo)m.Key.Method, m, constructorArguments));
        var methodSummaries = await Task.WhenAll(methodTasks).ConfigureAwait(false);

        foreach (var methodSummary in methodSummaries)
        {
            summary.Aggregate(methodSummary);
        }

        return summary;
    }

    protected override Task<RunSummary> RunTestMethodAsync(ITestMethod testMethod, IReflectionMethodInfo method,
        IEnumerable<IXunitTestCase> testCases, object[] constructorArguments)
        => new ParallelTestMethodRunner(testMethod, Class, method, testCases, DiagnosticMessageSink, MessageBus,
            new ExceptionAggregator(Aggregator), CancellationTokenSource, constructorArguments).RunAsync();

    private static Exception Unwrap(Exception ex)
    {
        while (true)
        {
            if (ex is not TargetInvocationException tiex || tiex.InnerException == null)
                return ex;

            ex = tiex.InnerException;
        }
    }
}

public class ParallelTestMethodRunner : XunitTestMethodRunner
{
    readonly object[] constructorArguments;
    readonly IMessageSink diagnosticMessageSink;

    public ParallelTestMethodRunner(ITestMethod testMethod, IReflectionTypeInfo @class, IReflectionMethodInfo method,
        IEnumerable<IXunitTestCase> testCases, IMessageSink diagnosticMessageSink, IMessageBus messageBus,
        ExceptionAggregator aggregator, CancellationTokenSource cancellationTokenSource, object[] constructorArguments)
        : base(testMethod, @class, method, testCases, diagnosticMessageSink, messageBus, aggregator,
            cancellationTokenSource, constructorArguments)
    {
        this.constructorArguments = constructorArguments;
        this.diagnosticMessageSink = diagnosticMessageSink;
    }

    // This method has been slightly modified from the original implementation to run tests in parallel
    // https://github.com/xunit/xunit/blob/2.4.2/src/xunit.execution/Sdk/Frameworks/Runners/TestMethodRunner.cs#L130-L142
    protected override async Task<RunSummary> RunTestCasesAsync()
    {
        var disableParallelization =
            TestMethod.TestClass.Class.GetCustomAttributes(typeof(DisableParallelizationAttribute)).Any()
            || TestMethod.Method.GetCustomAttributes(typeof(DisableParallelizationAttribute)).Any()
            || TestMethod.Method.GetCustomAttributes(typeof(MemberDataAttribute)).Any(a =>
                a.GetNamedArgument<bool>(nameof(MemberDataAttribute.DisableDiscoveryEnumeration)));

        if (disableParallelization)
            return await base.RunTestCasesAsync().ConfigureAwait(false);

        var summary = new RunSummary();

        var caseTasks = TestCases.Select(RunTestCaseAsync);
        var caseSummaries = await Task.WhenAll(caseTasks).ConfigureAwait(false);

        foreach (var caseSummary in caseSummaries)
        {
            summary.Aggregate(caseSummary);
        }

        return summary;
    }

    protected override async Task<RunSummary> RunTestCaseAsync(IXunitTestCase testCase)
    {
        // Create a new TestOutputHelper for each test case since they cannot be reused when running in parallel
        var args = constructorArguments.Select(a => a is TestOutputHelper ? new TestOutputHelper() : a).ToArray();

        var action = () => testCase.RunAsync(diagnosticMessageSink, MessageBus, args,
            new ExceptionAggregator(Aggregator), CancellationTokenSource);

        // Respect MaxParallelThreads by using the MaxConcurrencySyncContext if it exists, mimicking how collections are run
        // https://github.com/xunit/xunit/blob/2.4.2/src/xunit.execution/Sdk/Frameworks/Runners/XunitTestAssemblyRunner.cs#L169-L176
        if (SynchronizationContext.Current != null)
        {
            var scheduler = TaskScheduler.FromCurrentSynchronizationContext();
            return await Task.Factory
                .StartNew(action, CancellationTokenSource.Token,
                    TaskCreationOptions.DenyChildAttach | TaskCreationOptions.HideScheduler, scheduler).Unwrap()
                .ConfigureAwait(false);
        }

        return await Task.Run(action, CancellationTokenSource.Token).ConfigureAwait(false);
    }
}