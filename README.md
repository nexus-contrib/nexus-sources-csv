# Nexus.Sources.Csv

This data source extension makes it possible to read data files in the Csv format into Nexus.

To use it, put a `config.json` with the following sample content into the database root folder:

```json
{
  "/A/B/C": {
    "FileSourceGroups": {
      "raw": [{
        "PathSegments": [
          "'DATA'",
          "yyyy-MM"
        ],
        "FileTemplate": "yyyy-MM-dd_HH-mm-ss'.csv'",
        "FilePeriod": "00:00:10",
        "UtcOffset": "00:00:00",
        "AdditionalProperties": {
          "SamplePeriod": "00:00:01",
          "Separator": ";",
          "DecimalSeparator": ".",
          "InvalidValue": "999",
          "CodePage": 0,
          "HeaderRow": 1,
          "SkipColumns": [ 0, 1, 3 ],
          "UnitPattern": "in (.*)",
          "ReplaceNameRules": [
            {
              "Pattern": "Foo",
              "Replacement": "Bar"
            },
            {
              "Pattern": " in (.*)",
              "Replacement": ""
            }
          ]
        }
      }]
    }
  }
}
```

Please see the [tests](tests/Nexus.Sources.Csv.Tests) folder for a complete sample.