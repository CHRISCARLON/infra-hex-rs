# infra-hex-rs

A Rust library for fetching gas pipeline infrastructure data and aggregating it into H3 inspired hexagonal grids.

This uses a British National Grid implementation of hexagonal grids.

## What it does

1. **Fetches pipeline data** By bounding box
2. **Converts pipeline geometries** (LineStrings) into n3gb hexagon cells at a specified zoom
3. **Aggregates pipeline counts** per hexagon as arrow batches
4. **Exports results** as GeoParquet files (optional)

**Example of this library being used in a Python application:**

![infra-hex](https://github.com/user-attachments/assets/00e0e069-dc29-402b-8c24-bb073850413f)

## Setup

Set your Cadent API key:

```bash
export CADENT_API_KEY=your_api_key_here
```
