# Repository Guidelines

## Project Structure & Module Organization
This repository is a set of standalone Node.js scrapers kept at the root. `texas-sales-liens.js` and `texas-sales-liens2.js` enrich the `texas-future-sales*.csv` inputs, while `lgb-scrape-NoMetro.js` captures non-metro auction listings that feed the checked-in `lgb-*.json` snapshots. `Archive Codes/` holds retired enrichment experiments and certificates; treat it as read-only reference. Store captured HTML or API payloads that justify code changes in `debug_responses/` with timestamped filenames. Add new scripts beside the datasets they mutate so runners stay discoverable.

## Build, Test, and Development Commands
- `npm install` — installs axios, cheerio, puppeteer, csv helpers, and other shared dependencies.
- `TEST_LIMIT=5 node texas-sales-liens.js` — smoke-test lien parsing on five rows before hitting the full dataset.
- `node texas-sales-liens.js` — runs the complete lien enrichment and refreshes `texas-sales-liens.csv` plus debug artifacts.
- `node lgb-scrape-NoMetro.js > lgb-results.json` — re-pulls the non-metro snapshot and redirects stdout to the file you intend to update.
- `npm test` — currently fails intentionally; repoint it to `node --test` once real specs exist.

## Coding Style & Naming Conventions
The project is ESM-first (`type: module`), so stick to `import`/`export`, two-space indentation, and `const` for immutable references. Configuration objects stay in SCREAMING_SNAKE_CASE, while functions use camelCase verbs. Each entry script begins with a short comment that explains inputs, outputs, and run instructions—refresh that header whenever behavior changes. Keep logs actionable, reuse the existing `Memory/Run/Render` inline note style when editing `lgb-scrape-NoMetro.js`, and prefer async/await plus shared retry helpers for HTTP calls.

## Testing Guidelines
There is no automated harness yet, so rely on deterministic manual checks. Use `TEST_LIMIT`, county filters, or `TEST_LIMIT=1` dry runs to validate parsing, inspect resulting headers, and verify row counts with `wc -l filename.csv`. Save any surprising HTML or JSON responses inside `debug_responses/` and reference those files in your PR description. When you add utility modules, create `*.spec.mjs` tests and invoke them through `node --test`, then update the `npm test` script accordingly.

## Commit & Pull Request Guidelines
Recent history favors concise, imperative commit subjects (e.g., “cleaner version of texas enrichment”), so keep them under ~60 characters and only add body text for context. Every PR should list the dataset touched, the exact command used (including env vars), and whether large CSV/JSON artifacts changed size. Link the relevant issue, call out throttling or credential assumptions, and provide a small before/after sample so reviewers can verify results without rerunning the scraper blindly.
