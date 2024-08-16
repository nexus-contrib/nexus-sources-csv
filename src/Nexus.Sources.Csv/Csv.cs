using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

[ExtensionDescription(
    "Provides access to databases with CSV files.",
    "https://github.com/Apollo3zehn/nexus-sources-csv",
    "https://github.com/Apollo3zehn/nexus-sources-csv")]
public class Csv : StructuredFileDataSource
{
    private record CatalogDescription(
        string Title,
        Dictionary<string, IReadOnlyList<FileSource>> FileSourceGroups,
        JsonElement? AdditionalProperties);

    private record ReplaceNameRule(
        string Pattern,
        string Replacement
    );

    private record DateTimeModeOptions(
        int Column,
        string Pattern,
        TimeSpan TimestampOffset
    );

    private record AdditionalProperties(
        TimeSpan SamplePeriod,
        string? InvalidValue,
        int CodePage,
        int HeaderRow,
        string? SkipColumnPattern,
        string? UnitPattern,
        string? DefaultGroup,
        string? GroupPattern,
        string[]? CatalogSourceFiles,
        ReplaceNameRule[]? ReplaceNameRules,
        DateTimeModeOptions? DateTimeModeOptions,
        char Separator = ',',
        char DecimalSeparator = '.',
        int UnitRow = -1,
        int DataRow = -1);

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

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(
            catalogId => _config[catalogId].FileSourceGroups);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        if (path == "/")
            return Task.FromResult(_config.Select(entry => new CatalogRegistration(entry.Key, entry.Value.Title)).ToArray());

        else
            return Task.FromResult(Array.Empty<CatalogRegistration>());
    }

    protected override Task<ResourceCatalog> GetCatalogAsync(string catalogId, CancellationToken cancellationToken)
    {
        var catalogDescription = _config[catalogId];
        var catalog = new ResourceCatalog(id: catalogId);

        foreach (var (fileSourceId, fileSourceGroup) in catalogDescription.FileSourceGroups)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var newCatalogBuilder = new ResourceCatalogBuilder(id: catalogId);

                if (fileSource.AdditionalProperties is null)
                    continue;

                var additionalProperties = JsonSerializer
                    .Deserialize<AdditionalProperties>(fileSource.AdditionalProperties.Value)!;

                var filePaths = default(string[]);

                if (additionalProperties.CatalogSourceFiles is not null)
                {
                    filePaths = additionalProperties.CatalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = [filePath];
                }

                var encoding = Encoding.GetEncoding(additionalProperties.CodePage);

                foreach (var filePath in filePaths)
                {
                    using var reader = new StreamReader(File.OpenRead(filePath), encoding);

                    var (headerLine, unitLine) = ReadHeaderAndUnitLine(reader, additionalProperties);
                    var resourceProperties = GetResourceProperties(headerLine, unitLine, additionalProperties);

                    foreach (var resourceProperty in resourceProperties)
                    {
                        if (resourceProperty.Equals(default))
                            continue;

                        var (originalName, resourceId, unit, group) = resourceProperty;

                        group ??= additionalProperties.DefaultGroup;

                        // build representation
                        var representation = new Representation(
                            dataType: NexusDataType.FLOAT64,
                            samplePeriod: additionalProperties.SamplePeriod);

                        // build resource
                        var resourceBuilder = new ResourceBuilder(id: resourceId)
                            .WithFileSourceId(fileSourceId)
                            .WithOriginalName(originalName)
                            .AddRepresentation(representation);

                        if (unit is not null)
                            resourceBuilder.WithUnit(unit);

                        if (group is not null)
                            resourceBuilder.WithGroups(group);

                        newCatalogBuilder.AddResource(resourceBuilder.Build());
                    }

                    catalog = catalog.Merge(newCatalogBuilder.Build());

                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(
        ReadInfo info,
        StructuredFileReadRequest[] readRequests,
        CancellationToken cancellationToken)
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

            // find indices
            int[] indices;

            if (additionalProperties.HeaderRow == -1)
            {
                indices = GetIndices(info, readRequests);
            }

            else
            {
                var (headerLine, _) = ReadHeaderAndUnitLine(reader, additionalProperties);

                var parts = headerLine
                    .Split(additionalProperties.Separator)
                    .ToList();

                indices = readRequests
                    .Select(readRequest => parts.FindIndex(current => current == readRequest.OriginalName))
                    .ToArray();
            }

            // read
            var buffers = readRequests
                .Select(readRequest => new CastMemoryManager<byte, double>(readRequest.Data).Memory)
                .ToArray();

            if (additionalProperties.DateTimeModeOptions is null)
            {
                // seek
                for (int i = 0; i < info.FileOffset; i++)
                {
                    reader.ReadLine();
                }

                for (int i = 0; i < info.FileBlock; i++)
                {
                    var line = reader.ReadLine();

                    if (line is null)
                    {
                        Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                        return;
                    }

                    // for each read request
                    for (int j = 0; j < readRequests.Length; j++)
                    {
                        var index = indices[j];

                        if (index == -1)
                            continue;

                        if (!TryGetCell(line, index, additionalProperties.Separator, out var cell))
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        if (MemoryExtensions.Equals(cell, additionalProperties.InvalidValue, StringComparison.Ordinal))
                        {
                            buffers[j].Span[i] = double.NaN;
                        }

                        else
                        {
                            if (!double.TryParse(cell, NumberStyles.Float, nfi, out var value))
                                value = double.NaN;

                            buffers[j].Span[i] = value;
                        }
                    }
                }

                cancellationToken.ThrowIfCancellationRequested();

                // write status
                for (int i = 0; i < readRequests.Length; i++)
                {
                    readRequests[i]
                        .Status
                        .Span
                        .Fill(1);
                }
            }

            else
            {
                var samplePeriod = readRequests.First().CatalogItem.Representation.SamplePeriod;
                var (dateTimeColumn, dateTimePattern, timestampOffset) = additionalProperties.DateTimeModeOptions;

                string? line;

                while ((line = reader.ReadLine()) != null)
                {
                    // find buffer index using datetime
                    if (!TryGetCell(line, dateTimeColumn - 1, additionalProperties.Separator, out var dateTimeCell))
                    {
                        Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                        return;
                    };

                    var dateTime = DateTime
                        .ParseExact(dateTimeCell, dateTimePattern, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal)
                        .Add(-timestampOffset);

                    if (dateTime.Kind == DateTimeKind.Unspecified)
                        dateTime = DateTime.SpecifyKind(dateTime.Add(-info.FileSource.UtcOffset), DateTimeKind.Utc);

                    var i = (int)((dateTime - info.FileBegin).Ticks / samplePeriod.Ticks - info.FileOffset);

                    if (i < 0 || i >= info.FileBlock)
                        continue;

                    // for each read request
                    for (int j = 0; j < readRequests.Length; j++)
                    {
                        var index = indices[j];

                        if (index == -1)
                            continue;

                        if (!TryGetCell(line, index, additionalProperties.Separator, out var cell))
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        if (MemoryExtensions.Equals(cell, additionalProperties.InvalidValue, StringComparison.Ordinal))
                        {
                            buffers[j].Span[i] = double.NaN;
                        }

                        else
                        {
                            if (!double.TryParse(cell, NumberStyles.Float, nfi, out var value))
                                value = double.NaN;

                            buffers[j].Span[i] = value;
                            readRequests[j].Status.Span[i] = 1;
                        }
                    }
                }
            }
        }, cancellationToken);
    }

    protected virtual int[] GetIndices(ReadInfo info, StructuredFileReadRequest[] readRequests)
    {
        return readRequests
            .Select(readRequest => -1)
            .ToArray();
    }

    private static (string HeaderLine, string UnitLine) ReadHeaderAndUnitLine(
        StreamReader reader,
        AdditionalProperties additionalProperties)
    {
        if (additionalProperties.UnitRow < 0)
            additionalProperties = additionalProperties with { UnitRow = additionalProperties.HeaderRow };

        var maxRow = Math.Max(
            Math.Max(additionalProperties.HeaderRow, additionalProperties.UnitRow),
            additionalProperties.DataRow);

        string headerLine = default!;
        string unitLine = default!;

        for (int i = 0; i < maxRow; i++)
        {
            var line = reader.ReadLine() ?? throw new Exception("The file is incomplete.");

            if (i == (additionalProperties.HeaderRow - 1))
                headerLine = line;

            if (i == (additionalProperties.UnitRow - 1))
                unitLine = line;
        }

        return (headerLine, unitLine);
    }

    private static List<(string, string, string?, string?)> GetResourceProperties(
        string headerLine,
        string unitLine,
        AdditionalProperties additionalProperties)
    {
        // analyse header line
        var resourceProperties = new List<(string, string, string?, string?)>();
        var headerColumns = headerLine.Split(additionalProperties.Separator);
        var unitColumns = unitLine.Split(additionalProperties.Separator);

        for (int i = 0; i < headerColumns.Length; i++)
        {
            // skip columns
            var originalName = headerColumns[i];

            if (additionalProperties.SkipColumnPattern is not null)
            {
                if (Regex.IsMatch(originalName, additionalProperties.SkipColumnPattern))
                {
                    resourceProperties.Add(default);
                    continue;
                }
            }

            // try get unit
            var unit = default(string?);

            if (additionalProperties.UnitPattern is not null)
            {
                var match = Regex.Match(unitColumns[i], additionalProperties.UnitPattern);

                if (match.Success)
                    unit = match.Groups[1].Value;
            }

            else if (additionalProperties.UnitRow != -1)
            {
                unit = unitColumns[i];
            }

            // try get group
            var group = default(string?);

            if (additionalProperties.GroupPattern is not null)
            {
                var match = Regex.Match(originalName, additionalProperties.GroupPattern);

                if (match.Success)
                    group = match.Groups[1].Value;
            }

            // try get resource id
            var resourceId = FormatResourceId(originalName, additionalProperties.ReplaceNameRules);

            if (!TryEnforceNamingConvention(resourceId, out resourceId))
            {
                resourceProperties.Add(default);
                continue;
            }

            // 
            resourceProperties.Add((originalName, resourceId, unit, group));
        }

        return resourceProperties;
    }

    private static bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
    {
        newResourceId = resourceId;
        newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
        newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

        return Resource.ValidIdExpression.IsMatch(newResourceId);
    }

    private static string FormatResourceId(string id, ReplaceNameRule[]? replaceNameRules)
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool TryGetCell(ReadOnlySpan<char> line, int index, char separator, out ReadOnlySpan<char> cell)
    {
        cell = default;
        var slicedLine = line;

        for (int j = 0; j < index; j++)
        {
            if (slicedLine.Length > 0 && slicedLine[0] == '"')
            {
                if (TrySkipCellWithQuotes(slicedLine[1..], separator, out slicedLine))
                    continue;

                else
                    return false;
            }

            var separatorIndex = slicedLine.IndexOf(separator);

            if (separatorIndex == -1)
                return false;

            slicedLine = slicedLine[(separatorIndex + 1)..];
        }

        var nextSeparatorIndex = slicedLine.IndexOf(separator);

        if (nextSeparatorIndex == -1)
            cell = slicedLine;

        else
            cell = slicedLine[..nextSeparatorIndex];

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TrySkipCellWithQuotes(ReadOnlySpan<char> line, char separator, out ReadOnlySpan<char> slicedLine)
    {
        slicedLine = line;
        var needPartnerQuote = true;

        while (true)
        {
            if (slicedLine.Length == 0)
                return false; /* return false is correct because the calling method expects more cells after this one */

            if (needPartnerQuote)
            {
                var quoteIndex = slicedLine.IndexOf('"');

                if (quoteIndex == -1)
                    return false;

                slicedLine = slicedLine[(quoteIndex + 1)..];
                needPartnerQuote = false;
            }

            else
            {
                var nextCharIsSeparator = slicedLine[0] == separator;

                if (nextCharIsSeparator)
                {
                    slicedLine = slicedLine[1..];
                    return true;
                }

                else
                {
                    var nextCharIsQuote = slicedLine[0] == '"';

                    if (nextCharIsQuote)
                    {
                        slicedLine = slicedLine[1..];
                        needPartnerQuote = true;
                    }

                    else
                    {
                        return false; /* syntax error in CSV file */
                    }
                }
            }
        }
    }

    #endregion
}