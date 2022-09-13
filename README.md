# Nexus.Sources.Csv

This data source extension makes it possible to read data files in the Csv format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSources": [
      {
        "Name": "raw",
        "PathSegments": [
          "'DATA'",
          "yyyy-MM"
        ],
        "FileTemplate": "yyyy-MM-dd_HH-mm-ss'.dat'",
        "FilePeriod": "00:10:00",
        "UtcOffset": "00:00:00"
      }
    ]
  }
}
```

Please see the [tests](tests/Nexus.Sources.Csv.Tests) folder for a complete sample.