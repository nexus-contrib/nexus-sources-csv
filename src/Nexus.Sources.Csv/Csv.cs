using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

/// <summary>
/// Additional extension-specific settings.
/// </summary>
/// <param name="TitleMap">The catalog ID to title map. Add an entry here to specify a custom catalog title.</param>
/// <param name="AdditionalSettings">Additional settings.</param>
public record CsvSettings<TAdditionalSettings>(
    Dictionary<string, string> TitleMap,
    TAdditionalSettings AdditionalSettings
);

/// <summary>
/// Settings for the date/time mode.
/// </summary>
/// <param name="Column">The column to extract the date/time from.</param>
/// <param name="Pattern">The date/time pattern.</param>
/// <param name="TimestampOffset">The timestamp offset to apply.</param>
public record DateTimeModeOptions(
    int Column,
    string Pattern,
    TimeSpan TimestampOffset
);

/// <summary>
/// Additional file source settings.
/// </summary>
/// <param name="SamplePeriod">The period between samples.</param>
/// <param name="InvalidValue">The value to use for invalid entries.</param>
/// <param name="CodePage">The code page to use for decoding.</param>
/// <param name="HeaderRow">The row number of the header.</param>
/// <param name="ResourceIdPrefix">The prefix for resource IDs.</param>
/// <param name="SkipColumnPattern">The pattern for columns to skip.</param>
/// <param name="UnitPattern">The pattern for units.</param>
/// <param name="CatalogSourceFiles">The source files to populate the catalog with resources.</param>
/// <param name="DateTimeModeOptions">The options for date/time extraction.</param>
/// <param name="Separator">The character used to separate values in the CSV file. Default is ','.</param>
/// <param name="DecimalSeparator">The character used to separate decimal values. Default is '.'.</param>
/// <param name="UnitRow">The row number of the unit. Default is -1.</param>
/// <param name="DataRow">The row number of the data. Default is -1.</param>
public record CsvAdditionalFileSourceSettings(
    TimeSpan SamplePeriod,
    string? InvalidValue,
    int CodePage,
    int HeaderRow,
    string? ResourceIdPrefix,
    string? SkipColumnPattern,
    string? UnitPattern,
    string[]? CatalogSourceFiles,
    DateTimeModeOptions? DateTimeModeOptions,
    char Separator = ',',
    char DecimalSeparator = '.',
    int UnitRow = -1,
    int DataRow = -1
);

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

[ExtensionDescription(
    "Provides access to databases with CSV files.",
    "https://github.com/Apollo3zehn/nexus-sources-csv",
    "https://github.com/Apollo3zehn/nexus-sources-csv")]
public class Csv : Csv<object?>;

public abstract class Csv<TAdditionalSettings> 
    : StructuredFileDataSource<CsvSettings<TAdditionalSettings>, CsvAdditionalFileSourceSettings>
{
    static Csv()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken
    )
    {
        if (path == "/")
        {
            return Task.FromResult(Context.SourceConfiguration.FileSourceGroupsMap
                .Select(entry =>
                    {
                        Context.SourceConfiguration.AdditionalSettings.TitleMap.TryGetValue(entry.Key, out var title);
                        return new CatalogRegistration(entry.Key, title);
                    }
                ).ToArray());
        }

        else
        {
            return Task.FromResult(Array.Empty<CatalogRegistration>());
        }
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        var fileSourceGroupsMap = Context.SourceConfiguration.FileSourceGroupsMap[catalog.Id];

        foreach (var (fileSourceId, fileSourceGroup) in fileSourceGroupsMap)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var additionalSettings = fileSource.AdditionalSettings;
                var filePaths = default(string[]);

                if (additionalSettings.CatalogSourceFiles is not null)
                {
                    filePaths = additionalSettings.CatalogSourceFiles
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

                var encoding = Encoding.GetEncoding(additionalSettings.CodePage);

                foreach (var filePath in filePaths)
                {
                    using var reader = new StreamReader(File.OpenRead(filePath), encoding);

                    var (headerLine, unitLine) = ReadHeaderAndUnitLine(reader, additionalSettings);
                    var resourceProperties = GetResourceProperties(headerLine, unitLine, fileSource.AdditionalSettings);
                    var newCatalogBuilder = new ResourceCatalogBuilder(id: catalog.Id);

                    foreach (var resourceProperty in resourceProperties)
                    {
                        if (resourceProperty.Equals(default))
                            continue;

                        var (originalName, resourceId, unit) = resourceProperty;

                        // build representation
                        var representation = new Representation(
                            dataType: NexusDataType.FLOAT64,
                            samplePeriod: additionalSettings.SamplePeriod);

                        // build resource
                        var resourceBuilder = new ResourceBuilder(id: resourceId)
                            .WithFileSourceId(fileSourceId)
                            .WithOriginalName(originalName)
                            .AddRepresentation(representation);

                        if (unit is not null)
                            resourceBuilder.WithUnit(unit);

                        newCatalogBuilder.AddResource(resourceBuilder.Build());
                    }

                    catalog = catalog.Merge(newCatalogBuilder.Build());
                }
            }
        }

        return Task.FromResult(catalog);
    }

    protected override Task ReadAsync(
        ReadInfo<CsvAdditionalFileSourceSettings> info,
        ReadRequest[] readRequests,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            var additionalSettings = info.FileSource.AdditionalSettings;

            // number format info
            var nfi = new NumberFormatInfo()
            {
                NumberDecimalSeparator = additionalSettings.DecimalSeparator.ToString()
            };

            // encoding / reader
            var encoding = Encoding.GetEncoding(additionalSettings.CodePage);
            using var reader = new StreamReader(File.OpenRead(info.FilePath), encoding);

            // find indices
            int[] indices;

            if (additionalSettings.HeaderRow == -1)
            {
                indices = GetIndices(info, readRequests);
            }

            else
            {
                var (headerLine, _) = ReadHeaderAndUnitLine(reader, additionalSettings);

                var parts = headerLine
                    .Split(additionalSettings.Separator)
                    .ToList();

                indices = readRequests
                    .Select(readRequest => parts.FindIndex(current => current == readRequest.OriginalResourceName))
                    .ToArray();
            }

            // read
            var buffers = readRequests
                .Select(readRequest => new CastMemoryManager<byte, double>(readRequest.Data).Memory)
                .ToArray();

            if (additionalSettings.DateTimeModeOptions is null)
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

                        if (!TryGetCell(line, index, additionalSettings.Separator, out var cell))
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        if (MemoryExtensions.Equals(cell, additionalSettings.InvalidValue, StringComparison.Ordinal))
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
                var (dateTimeColumn, dateTimePattern, timestampOffset) = additionalSettings.DateTimeModeOptions;

                string? line;

                while ((line = reader.ReadLine()) != null)
                {
                    // find buffer index using datetime
                    if (!TryGetCell(line, dateTimeColumn - 1, additionalSettings.Separator, out var dateTimeCell))
                    {
                        Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                        return;
                    }
                    ;

                    var dateTime = DateTime
                        .ParseExact(dateTimeCell, dateTimePattern, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal)
                        .Add(-timestampOffset);

                    if (dateTime.Kind == DateTimeKind.Unspecified)
                        dateTime = DateTime.SpecifyKind(dateTime.Add(-info.FileSource.UtcOffset), DateTimeKind.Utc);

                    var i = (int)((dateTime - info.RegularFileBegin).Ticks / samplePeriod.Ticks - info.FileOffset);

                    if (i < 0 || i >= info.FileBlock)
                        continue;

                    // for each read request
                    for (int j = 0; j < readRequests.Length; j++)
                    {
                        var index = indices[j];

                        if (index == -1)
                            continue;

                        if (!TryGetCell(line, index, additionalSettings.Separator, out var cell))
                        {
                            Logger.LogDebug("The actual buffer size does not match the expected size, which indicates an incomplete file");
                            return;
                        }

                        if (MemoryExtensions.Equals(cell, additionalSettings.InvalidValue, StringComparison.Ordinal))
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

    protected virtual int[] GetIndices(ReadInfo<CsvAdditionalFileSourceSettings> info, ReadRequest[] readRequests)
    {
        return readRequests
            .Select(readRequest => -1)
            .ToArray();
    }

    private static (string HeaderLine, string UnitLine) ReadHeaderAndUnitLine(
        StreamReader reader,
        CsvAdditionalFileSourceSettings additionalSettings)
    {
        if (additionalSettings.UnitRow < 0)
            additionalSettings = additionalSettings with { UnitRow = additionalSettings.HeaderRow };

        var maxRow = Math.Max(
            Math.Max(additionalSettings.HeaderRow, additionalSettings.UnitRow),
            additionalSettings.DataRow);

        string headerLine = default!;
        string unitLine = default!;

        for (int i = 0; i < maxRow; i++)
        {
            var line = reader.ReadLine() ?? throw new Exception("The file is incomplete.");

            if (i == (additionalSettings.HeaderRow - 1))
                headerLine = line;

            if (i == (additionalSettings.UnitRow - 1))
                unitLine = line;
        }

        return (headerLine, unitLine);
    }

    private static List<(string, string, string?)> GetResourceProperties(
        string headerLine,
        string unitLine,
        CsvAdditionalFileSourceSettings additionalSettings)
    {
        // analyse header line
        var resourceProperties = new List<(string, string, string?)>();
        var headerColumns = headerLine.Split(additionalSettings.Separator);
        var unitColumns = unitLine.Split(additionalSettings.Separator);

        for (int i = 0; i < headerColumns.Length; i++)
        {
            // skip columns
            var originalName = headerColumns[i];

            if (additionalSettings.SkipColumnPattern is not null)
            {
                if (Regex.IsMatch(originalName, additionalSettings.SkipColumnPattern))
                {
                    resourceProperties.Add(default);
                    continue;
                }
            }

            // try get unit
            var unit = default(string?);

            if (additionalSettings.UnitPattern is not null)
            {
                var match = Regex.Match(unitColumns[i], additionalSettings.UnitPattern);

                if (match.Success)
                    unit = match.Groups[1].Value;
            }

            else if (additionalSettings.UnitRow != -1)
            {
                unit = unitColumns[i];
            }

            // try get resource id
            var prefixedOriginalName = additionalSettings.ResourceIdPrefix + originalName;

            if (!TryEnforceNamingConvention(prefixedOriginalName, out var resourceId))
            {
                resourceProperties.Add(default);
                continue;
            }

            // 
            resourceProperties.Add((originalName, resourceId, unit));
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
}

#pragma warning restore CS1591 // Missing XML comment for publicly visible type or member