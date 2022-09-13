using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Nexus.Sources
{
    [ExtensionDescription(
        "Provides access to databases with CSV files.",
        "https://github.com/Apollo3zehn/nexus-sources-csv",
        "https://github.com/Apollo3zehn/nexus-sources-csv")]
    public class Csv : StructuredFileDataSource
    {
        record CatalogDescription(
            string Title,
            Dictionary<string, FileSource> FileSources, 
            JsonElement? AdditionalProperties);

        record ReplaceNameRule(
            string Pattern,
            string Replacement);

        record AdditionalProperties(
            TimeSpan SamplePeriod,
            string InvalidValue,
            int CodePage,
            int HeaderRow,
            List<int>? SkipColumns,
            string? UnitPattern,
            List<ReplaceNameRule>? ReplaceNameRules,
            char Separator = ',',
            char DecimalSeparator = '.');

        #region Fields

        private Dictionary<string, CatalogDescription> _config = default!;

        #endregion

        #region Constructors

        static Csv()
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }

        #endregion

        #region Methods

        protected override async Task InitializeAsync(CancellationToken cancellationToken)
        {
            var configFilePath = Path.Combine(Root, "config.json");

            if (!File.Exists(configFilePath))
                throw new Exception($"Configuration file {configFilePath} not found.");

            var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
            _config = JsonSerializer.Deserialize<Dictionary<string, CatalogDescription>>(jsonString) ?? throw new Exception("config is null");
        }

        protected override Task<Func<string, Dictionary<string, FileSource>>> GetFileSourceProviderAsync(
            CancellationToken cancellationToken)
        {
            return Task.FromResult<Func<string, Dictionary<string, FileSource>>>(
                catalogId => _config[catalogId].FileSources);
        }

        protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
        {
            if (path == "/")
                return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

            else
                return Task.FromResult(new CatalogRegistration[0]);
        }

        protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
        {
            var catalogDescription = _config[catalogId];
            var catalog = new ResourceCatalog(id: catalogId);

            foreach (var (fileSourceId, fileSource) in catalogDescription.FileSources)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var newCatalogBuilder = new ResourceCatalogBuilder(id: catalogId);

                if (fileSource.AdditionalProperties is null)
                    continue;

                var additionalProperties = JsonSerializer
                    .Deserialize<AdditionalProperties>(fileSource.AdditionalProperties.Value)!;

                if (!TryGetFirstFile(fileSource, out var filePath))
                    continue;

                var encoding = Encoding.GetEncoding(additionalProperties.CodePage);
                using var reader = new StreamReader(File.OpenRead(filePath), encoding);

                var resourceProperties = GetResourceProperties(reader, additionalProperties);

                foreach (var resourceProperty in resourceProperties)
                {
                    if (resourceProperty.Equals(default((string, string?))))
                        continue;

                    var (resourceId, unit) = resourceProperty;

                    // build representation
                    var representation = new Representation(
                        dataType: NexusDataType.FLOAT64,
                        samplePeriod: additionalProperties.SamplePeriod);

                    // build resource
                    var resourceBuilder = new ResourceBuilder(id: resourceId)
                        .WithGroups(fileSourceId)
                        .WithProperty(FileSourceKey, fileSourceId)
                        .AddRepresentation(representation);

                    if (unit is not null)
                        resourceBuilder.WithUnit(unit);

                    newCatalogBuilder.AddResource(resourceBuilder.Build());
                }

                catalog = catalog.Merge(newCatalogBuilder.Build());
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                if (info.FileSource.AdditionalProperties is null)
                    return;

                var additionalProperties = JsonSerializer
                    .Deserialize<AdditionalProperties>(info.FileSource.AdditionalProperties.Value)!;

                // number format info
                var nfi = new NumberFormatInfo()
                {
                    NumberDecimalSeparator = additionalProperties.DecimalSeparator.ToString()
                };

                // encoding / reader
                var encoding = Encoding.GetEncoding(additionalProperties.CodePage);
                using var reader = new StreamReader(File.OpenRead(info.FilePath), encoding);

                // resource properties
                var resourceProperties = GetResourceProperties(reader, additionalProperties);

                // find index
                var index = resourceProperties
                    .FindIndex((resourceProperty => resourceProperty.Item1 == info.CatalogItem.Resource.Id));

                if (index > -1)
                {
                    // seek
                    for (int i = 0; i < info.FileOffset; i++)
                    {
                        reader.ReadLine();
                    }

                    // read
                    var buffer = new double[info.FileBlock];
                    var byteBuffer = MemoryMarshal.AsBytes(buffer.AsSpan());

                    for (int i = 0; i < info.FileBlock; i++)
                    {
                        var line = reader.ReadLine();

                        if (line is null)
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        var parts = line.Split(additionalProperties.Separator, index + 2);

                        if (parts.Length < (index + 1))
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        var cell = parts[index];

                        if (cell == additionalProperties.InvalidValue)
                        {
                            buffer[i] = double.NaN;
                        }

                        else
                        {
                            if (!double.TryParse(cell, NumberStyles.Float, nfi, out var value))
                                value = double.NaN;

                            buffer[i] = value;
                        }
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    // write data
                    byteBuffer
                        .CopyTo(info.Data.Span);

                    info
                        .Status
                        .Span
                        .Fill(1);
                }
            });
        }

        private List<(string, string?)> GetResourceProperties(
            StreamReader reader,
            AdditionalProperties additionalProperties)
        {
            // read header line
            for (int i = 0; i < additionalProperties.HeaderRow; i++)
            {
                var skippedLine = reader.ReadLine();

                if (skippedLine is null)
                    throw new Exception("The file is incomplete.");
            }

            var headerLine = reader.ReadLine();

            if (headerLine is null)
                throw new Exception("The file is incomplete.");

            // analyse header line
            var resourceProperties = new List<(string, string?)>();
            var columns = headerLine.Split(additionalProperties.Separator);

            for (int i = 0; i < columns.Length; i++)
            {
                // skip columns
                var column = columns[i];

                if (additionalProperties.SkipColumns is not null &&
                    additionalProperties.SkipColumns.Contains(i))
                {
                    resourceProperties.Add(default);
                    continue;
                }

                // try get unit
                var unit = default(string?);

                if (additionalProperties.UnitPattern is not null)
                {
                    var match = Regex.Match(column, additionalProperties.UnitPattern);

                    if (match.Success)
                        unit = match.Groups[1].Value;
                }   

                // try get resource id
                var resourceId = FormatResourceId(column, additionalProperties.ReplaceNameRules);

                if (!TryEnforceNamingConvention(resourceId, out resourceId))
                {
                    resourceProperties.Add(default);
                    continue;
                }

                // 
                resourceProperties.Add((resourceId, unit));
            }

            return resourceProperties;
        }

        private bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
        {
            newResourceId = resourceId;
            newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
            newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

            return Resource.ValidIdExpression.IsMatch(newResourceId);
        }

        private string FormatResourceId(string id, List<ReplaceNameRule>? replaceNameRules)
        {
            if (replaceNameRules is not null)
            {
                foreach (var rule in replaceNameRules)
                {
                    id = Regex.Replace(id, rule.Pattern, rule.Replacement);
                }
            }

            return id;
        }

        #endregion
    }
}
