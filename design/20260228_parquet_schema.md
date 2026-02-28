# Lichess eval JSONL -> Parquet schema

Source used (official):
- https://database.lichess.org/ (section for `lichess_db_eval.jsonl.zst`)

The site defines one JSON object per line with this structure:
- `fen`
- `evals[]`
  - `knodes`
  - `depth`
  - `pvs[]`
    - `cp` (omitted when mate is certain)
    - `mate` (omitted when mate is not certain)
    - `line`

## Parquet logical schema (nested)

```text
root
|-- fen: STRING (required)
|-- evals: LIST<STRUCT> (required)
|   |-- element: STRUCT
|   |   |-- knodes: INT64 (required)
|   |   |-- depth: INT64 (required)
|   |   |-- pvs: LIST<STRUCT> (required)
|   |   |   |-- element: STRUCT
|   |   |   |   |-- cp: INT64 (nullable)
|   |   |   |   |-- mate: INT64 (nullable)
|   |   |   |   |-- line: STRING (required)
```

## Notes from source

- `fen` contains only: pieces, active color, castling rights, and en passant square.
- `evals` is a list of evaluations ordered by number of PVs.
- `line` is principal variation in UCI_Chess960 format.
- `cp` and `mate` must both be nullable because each can be omitted depending on mate certainty.
- The source specifies these numeric fields as JSON numbers/count/depth values, but does not publish bit width. `INT64` is used as a conservative parquet integer mapping.
