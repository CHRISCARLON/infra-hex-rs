# infra-hex-rs

A Rust tool for fetching gas pipeline infrastructure data and aggregating it into H3 inspired hexagonal grids.

This uses a British National Grid implementation of hexagonal grids.

## What it does

1. **Fetches pipeline data** from Cadent's OpenDataSoft API by bounding box
2. **Converts pipeline geometries** (LineStrings) into n3gb hexagon cells at a specified zoom
3. **Aggregates pipeline counts** per hexagon
4. **Exports results** as GeoParquet files (optional)

The default example fetches gas pipeline infrastructure for North London and outputs a hexagonal summary at zoom level 10.

## Features

- Async parallel API fetching with rate limiting
- Efficient hex grid processing 
- Apache Arrow-based in-memory processing
- GeoParquet output for spatial analysis tools like QGIS

## Setup

Set your Cadent API key:

```bash
export CADENT_API_KEY=your_api_key_here
```
