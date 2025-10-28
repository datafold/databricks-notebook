# DBX DMA Translations Notebook

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

## Debugging datafold-sdk

Create a `.env` file copied from `.env.example` and set `LOCAL_DATAFOLD_SDK_PATH` pointing to your local `datafold-sdk` repo.
Jupyter notebook will then install using `pip install -e` which allows editing the sdk.
To refresh the datafold-sdk to the latest version with your local changes just install it via the juniper notebook.
