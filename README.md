# Databricks Notebook

## Setup

Install dependencies:

```bash
uv sync
```

## Running the Notebook

Start Jupyter Notebook:

```bash
uv run jupyter notebook
```

The server will start and provide a URL with an access token to open in your browser.

## Local Development

Create a `.env` file copied from `.env.example` and set `LOCAL_DATABRICKS_NOTEBOOK_PATH` pointing to this repo.
Jupyter notebook will then install using `pip install -e` which allows editing the python code.
The code gets automatically refreshed every time you run a cell.
