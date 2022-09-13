using Nexus.DataModel;
using Nexus.Extensibility;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

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

        #region Fields

        private Dictionary<string, CatalogDescription> _config = default!;

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
                var filePaths = default(string[]);
                var catalogSourceFiles = fileSource.AdditionalProperties.GetStringArray("CatalogSourceFiles");

                if (catalogSourceFiles is not null)
                {
                    filePaths = catalogSourceFiles
                        .Where(filePath => filePath is not null)
                        .Select(filePath => Path.Combine(Root, filePath!))
                        .ToArray();
                }
                else
                {
                    if (!TryGetFirstFile(fileSource, out var filePath))
                        continue;

                    filePaths = new[] { filePath };
                }

                cancellationToken.ThrowIfCancellationRequested();

                foreach (var filePath in filePaths)
                {
                    var resources = GetResources(fileSourceId);

                    var newCatalog = new ResourceCatalogBuilder(id: catalogId)
                        .AddResources(resources)
                        .Build();

                    catalog = catalog.Merge(newCatalog);
                }
            }

            return Task.FromResult(catalog);
        }

        protected override Task ReadSingleAsync(ReadInfo info, CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                // to be implemented
            });
        }

        private List<Resource> GetResources(string fileSourceId)
        {
            throw new NotImplementedException();
        }

        private bool TryEnforceNamingConvention(string resourceId, [NotNullWhen(returnValue: true)] out string newResourceId)
        {
            newResourceId = resourceId;
            newResourceId = Resource.InvalidIdCharsExpression.Replace(newResourceId, "");
            newResourceId = Resource.InvalidIdStartCharsExpression.Replace(newResourceId, "");

            return Resource.ValidIdExpression.IsMatch(newResourceId);
        }

        #endregion
    }
}
