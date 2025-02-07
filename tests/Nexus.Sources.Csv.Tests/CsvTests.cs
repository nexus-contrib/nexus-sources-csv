using System.Runtime.InteropServices;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using Xunit;

namespace Nexus.Sources.Tests;

using MySettings = StructuredFileDataSourceSettings<CsvSettings, CsvAdditionalFileSourceSettings>;

public class CsvTests
{
    [Fact]
    public async Task ProvidesCatalog()
    {
        // arrange
        var dataSource = (IDataSource<MySettings>)new Csv();
        var context = BuildContext();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var actual = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var actualIds = actual.Resources!.Select(resource => resource.Id).ToList();
        var actualUnits = actual.Resources!.Select(resource => resource.Properties?.GetStringValue("unit")).ToList();
        var (begin, end) = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

        // assert
        var expectedIds = new List<string>() { "ThisIsTheFooVariable", "Anything" };
        var expectedUnits = new List<string>() { "m/s", "°C" };
        var expectedGroups = new List<string>() { "raw", "raw" };
        var expectedStartDate = new DateTime(2020, 01, 01, 00, 00, 00);
        var expectedEndDate = new DateTime(2020, 01, 01, 00, 00, 10);

        Assert.True(expectedIds.SequenceEqual(actualIds.Take(2)));
        Assert.True(expectedUnits.SequenceEqual(actualUnits.Take(2)));
        Assert.Equal(expectedStartDate, begin);
        Assert.Equal(expectedEndDate, end);
    }

    [Fact]
    public async Task CanRead_Equidistant()
    {
        // arrange
        var dataSource = (IDataSource<MySettings>)new Csv();
        var context = BuildContext();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalog = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var resource1 = catalog.Resources![0];
        var resource2 = catalog.Resources![1];
        var representation1 = resource1!.Representations![0];
        var representation2 = resource2!.Representations![0];

        var catalogItem1 = new CatalogItem(catalog, resource1, representation1, default);
        var catalogItem2 = new CatalogItem(catalog, resource2, representation2, default);

        var begin = new DateTime(2020, 01, 01, 0, 0, 1, DateTimeKind.Utc);
        var end = new DateTime(2020, 01, 01, 0, 0, 11, DateTimeKind.Utc);
        var (data1, status1) = ExtensibilityUtilities.CreateBuffers(representation1, begin, end);
        var (data2, status2) = ExtensibilityUtilities.CreateBuffers(representation2, begin, end);

        var result1 = new ReadRequest(resource1.Id, catalogItem1, data1, status1);
        var result2 = new ReadRequest(resource2.Id, catalogItem2, data2, status2);
        await dataSource.ReadAsync(begin, end, [result1, result2], default!, new Progress<double>(), CancellationToken.None);

        // assert
        void DoAssert()
        {
            // result 1
            var data1 = MemoryMarshal.Cast<byte, double>(result1.Data.Span);

            Assert.Equal(2e9, data1[0]);
            Assert.Equal(-10.34e-3, data1[1]);
            Assert.Equal(4, data1[2]);
            Assert.Equal(5, data1[3]);
            Assert.Equal(6.99, data1[4]);
            Assert.Equal(7.99, data1[5]);
            Assert.Equal(8.99, data1[6]);
            Assert.Equal(9.99, data1[7]);
            Assert.Equal(10.99, data1[8]);

            Assert.Equal(1, result1.Status.Span[0]);
            Assert.Equal(1, result1.Status.Span[8]);
            Assert.Equal(0, result1.Status.Span[9]);

            // result 2
            var data2 = MemoryMarshal.Cast<byte, double>(result2.Data.Span);

            Assert.Equal(double.NaN, data2[0]);
            Assert.Equal(2, data2[1]);
            Assert.Equal(3, data2[2]);
            Assert.Equal(double.NaN, data2[3]);
            Assert.Equal(5, data2[4]);
            Assert.Equal(6, data2[5]);
            Assert.Equal(7, data2[6]);
            Assert.Equal(8, data2[7]);
            Assert.Equal(9, data2[8]);

            Assert.Equal(1, result2.Status.Span[0]);
            Assert.Equal(1, result2.Status.Span[8]);
            Assert.Equal(0, result2.Status.Span[9]);
        }

        DoAssert();
    }

    [Fact]
    public async Task CanRead_DateTime()
    {
        // arrange
        var dataSource = (IDataSource<MySettings>)new Csv();
        var context = BuildContext();

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // act
        var catalog = await dataSource.EnrichCatalogAsync(new("/A/B/C"), CancellationToken.None);
        var resource = catalog.Resources![2];
        var representation = resource!.Representations![0];
        var catalogItem = new CatalogItem(catalog, resource, representation, default);

        var begin = new DateTime(2020, 01, 01, 0, 0, 1, DateTimeKind.Utc);
        var end = new DateTime(2020, 01, 01, 0, 0, 11, DateTimeKind.Utc);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var result = new ReadRequest(resource.Id, catalogItem, data, status);
        await dataSource.ReadAsync(begin, end, [result], default!, new Progress<double>(), CancellationToken.None);

        // assert
        void DoAssert()
        {
            // result 1
            var data1 = MemoryMarshal.Cast<byte, double>(result.Data.Span);

            Assert.Equal(2e9, data1[0]);
            Assert.Equal(-10.34e-3, data1[1]);
            Assert.Equal(4, data1[2]);
            Assert.Equal(5, data1[3]);
            Assert.Equal(0, data1[4]);
            Assert.Equal(6.99, data1[5]);
            Assert.Equal(7.99, data1[6]);
            Assert.Equal(8.99, data1[7]);
            Assert.Equal(9.99, data1[8]);

            Assert.Equal(1, result.Status.Span[0]);
            Assert.Equal(0, result.Status.Span[4]);
            Assert.Equal(1, result.Status.Span[8]);
            Assert.Equal(0, result.Status.Span[9]);
        }

        DoAssert();
    }

    [Theory]
    [InlineData("1.2,3.4,4.5", 2, "4.5")]
    [InlineData("\".,.\",1,abc", 2, "abc")]
    [InlineData("1,\".,.\",abc", 2, "abc")]
    [InlineData("1,abc,\".,.\"", 1, "abc")]
    [InlineData("\"ab,\"\"cd,e\"\"f\",1.20", 1, "1.20")]
    public void CanGetCell(string line, int index, string expected)
    {
        // Act
        var success = Csv.TryGetCell(line, index, ',', out var actual);

        // Assert
        Assert.True(success);
        Assert.Equal(expected, actual.ToString());
    }

    private static DataSourceContext<MySettings> BuildContext()
    {
        var configFilePath = Path.Combine("Database", "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"The configuration file does not exist on path {configFilePath}.");

        var jsonString = File.ReadAllText(configFilePath);
        var sourceConfiguration = JsonSerializer.Deserialize<MySettings>(jsonString, JsonSerializerOptions.Web)!;

        var context = new DataSourceContext<MySettings>(
            ResourceLocator: new Uri("Database", UriKind.Relative),
            SourceConfiguration: sourceConfiguration,
            RequestConfiguration: default!
        );

        return context;
    }
}