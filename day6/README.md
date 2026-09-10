# Day 6 — Word Count

## Tasks
- Implement classic Word Count.
- Explain flatMap -> map -> reduceByKey.
- Modify it to produce case-insensitive counts.
- Ignore punctuation and empty words.
- Scenario: Find the top 10 most frequent words in application logs.

## What I learned
- Classic Word Count pipeline: `flatMap` (split lines into words) -> `map` (word -> (word, 1)) -> `reduceByKey` (sum counts per word).
- `flatMap` flattens results from multiple lines into a single word list; a plain `map` would leave nested arrays.
- Case-insensitive counting is done by lowercasing each word before counting, so "User" and "user" merge into one count.
- Punctuation/empty words are removed using `replaceAll("[^a-z0-9]", "")` and `filter(_.nonEmpty)` before counting.
- `sortBy(_._2, ascending = false).take(10)` gives the top N most frequent words.

## How to run
```bash
sbt run
```
