# exsecutor-finetune.streamdb

A StreamDB v3 container holding the fine-tuning dataset of
[ALH477/exsecutor](https://github.com/ALH477/exsecutor) (`datasets/finetune/`)
and its provenance, keyed by path. It is here as a real-world use of StreamDB:
about a hundred documents of mixed size, a path-shaped key space, and a suffix
search that is useful (`.exsc` lists the case sources).

It was written with this repository's **C** library (`C/`, `streamdb_insert`
then `streamdb_flush`) by `tools/streamdb/pack.py` in the Exsecutor repository.

## Keys

| key | what |
|---|---|
| `/exsecutor/finetune/all.jsonl`, `train.jsonl`, `validation.jsonl` | the records, chat format, one JSON object per line |
| `/exsecutor/finetune/lexicon.json` | the structured lexicon the records were built from |
| `/exsecutor/finetune/manifest.json` | counts by task and by evidence, SHA-256 of every input and output |
| `/exsecutor/finetune/README.md` | the dataset's datasheet: what is and is not evidenced |
| `/exsecutor/finetune/src/keywords.json`, `qa.json` | editorial glosses and Q&A |
| `/exsecutor/finetune/src/cases/<id>.exsc` | the hand-written programs, each run through the real compiler |
| `/exsecutor/tools/gen-finetune.py` | the generator |
| `/exsecutor/provenance/index.json` | every key's SHA-256 and size, the Exsecutor git commit, and the SHA-256 of the spec the dataset was generated from |

`/exsecutor/provenance/index.json` is the place to start. Its `not_included`
field says what is **not** here: eval results, training runs and agent-loop
transcripts, none of which exist yet.

## Reading it

C:

```c
StreamDB* db = streamdb_init("exsecutor-finetune.streamdb", 0);
size_t n;
const unsigned char* k = (const unsigned char*)"/exsecutor/provenance/index.json";
char* idx = streamdb_get(db, k, strlen((const char*)k), &n);   /* caller frees */
StreamDBResult* r = streamdb_suffix_search(db, (const unsigned char*)".exsc", 5);
/* ... walk r->next ..., then streamdb_free_results(r) */
streamdb_free(db);
```

Python, through `ctypes` over `libstreamdb.so`: see `tools/streamdb/pack.py` in
the Exsecutor repository, which does exactly this for its read-back check
(`--check FILE` compares the container with a working tree).

## What was verified

When it was packed: the container was closed and reopened; every one of the 102
documents was read back and its SHA-256 compared with the source file; and the
suffix search for `.exsc` returned the same set of keys as the case files.
A byte flipped inside a container was caught by that same check.

## What was not

- **The Rust implementation has not read this file.** The project says the two
  implementations share the v3 on-disk format; that was not exercised here.
  Only the C library wrote and read it.
- **The file is not byte-reproducible.** StreamDB assigns each document a UUID,
  so packing the same inputs twice gives different files holding identical
  documents. The check compares documents, not file bytes.
- **Correctness of the records is not claimed by the container.** What the
  dataset itself does and does not evidence is in
  `/exsecutor/finetune/README.md`. In short: the code examples were run through
  the Exsecutor compiler; the prose around them was reviewed against the spec,
  not machine-checked.

## Licence

This repository is LGPL-3.0. The container's contents are from the Exsecutor
repository, which is GPL-3.0-or-later with an output exception
(`LICENSE.EXCEPTION` there). The two are recorded in the provenance index. Which
terms govern the combination is for the repository owner to decide, and this
README does not decide it.
