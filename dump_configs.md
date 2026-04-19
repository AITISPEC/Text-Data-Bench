# Dump: configs

**Root:** `C:\Users\AITISPEC\miniconda3\envs\TextDataBench\py\configs`

---

## 📄 default.yaml

```yaml
pipeline:
  prefer_gpu: true
  llm_model_path: "models/Qwen2-500M-Instruct-Q8_0.gguf"
  llm_ctx: 8192
  batch_size: 500

filters:
  min_length: 10
  max_length: 10000
  remove_empty: true

dedup:
  exact: true
  fuzzy: true
  fuzzy_threshold: 0.85
  num_perm: 128

balance:
  strategy: "stratified"  # stratified | uniform
  group_col: "category"   # optional
  seed: 42

output:
  format: "parquet"
  report_path: "output/report.md"
```

---

## 📄 parsers.yaml

```yaml
parsers:
  - handler: polars_parquet
    extensions: [".parquet"]
    kwargs: {}
  - handler: polars_csv
    extensions: [".csv"]
    kwargs: { try_parse_dates: true }
  - handler: polars_tsv
    extensions: [".tsv"]
    kwargs: { separator: "\t", try_parse_dates: true }
  - handler: polars_ipc
    extensions: [".feather", ".arrow"]
    kwargs: {}
  - handler: polars_excel
    extensions: [".xlsx", ".xls"]
    kwargs: {}
  - handler: polars_json
    extensions: [".json"]
    kwargs: {}
  - handler: polars_jsonl
    extensions: [".jsonl", ".jsonlines"]
    kwargs: {}
  - handler: text_lines
    extensions: [".txt", ".md"]
    kwargs: { encoding: "utf-8", strip: true }
  - handler: xml_simple
    extensions: [".xml"]
    kwargs: { tag_col: "xml_tag", content_col: "xml_text" }
  - handler: pickle_safe
    extensions: [".pkl", ".pickle"]
    kwargs: { trusted_only: true }
  - handler: llm_fallback
    extensions: ["*"]
    kwargs: {}
```

---

## 📄 pipeline.yaml

```yaml
pipeline:
  prefer_gpu: true
  text_col: null
filters:
  min_length: 10
  max_length: 10000
  remove_empty: true
dedup:
  exact: true
  fuzzy: true
  fuzzy_threshold: 0.85
  num_perm: 128
balance:
  strategy: "stratified"
  group_col: null
  seed: 42
output:
  format: "parquet"
  report_path: "output/report.md"
logging:
  level: "INFO"
  json_format: false
```

---

