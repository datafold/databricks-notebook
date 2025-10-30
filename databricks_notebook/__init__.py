import time
import difflib
from typing import List, Dict, Tuple
from databricks_notebook.utils import prepare_api_url, prepare_headers, post_data, get_data


__all__ = ['create_organization', 'translate_queries', 'view_translation_results_as_html', 'translate_queries_and_render_results']

DEFAULT_HOST = "https://app.datafold.com"

_notebook_host = None
_current_api_key = None

def _get_host(host: str | None) -> str:
    """Get the host to use, checking notebook-level variable first.
    If a host is explicitly provided, update _notebook_host for future calls.
    """
    global _notebook_host
    if host is not None:
        _notebook_host = host
        return host
    if _notebook_host is not None:
        return _notebook_host
    return DEFAULT_HOST

def _get_current_api_key(org_token: str | None, host: str | None = None) -> str | None:
    """Get the current API key."""
    global _current_api_key
    if _current_api_key is not None:
        return _current_api_key
    elif org_token is not None:
        host = _get_host(host)
        api_key, org_id = create_organization(org_token, host)
        _set_current_api_key(api_key)
        return api_key
    else:
        return None

def _set_current_api_key(api_key: str) -> None:
    """Set the current API key."""
    global _current_api_key
    _current_api_key = api_key

def create_organization(org_token: str, host: str | None = None) -> Tuple[str, int]:
    """
    Call the /org API endpoint to create a new organization.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        org_token: Donor org token from where to copy the organization from

    Returns:
        Tuple of (api_key, org_id)
    """
    host = _get_host(host)
    url = prepare_api_url(host, "org")
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {org_token}"
    }

    print("Creating Organization...")

    response = post_data(url, headers=headers)
    result = response.json()
    api_key = result['api_token']
    _set_current_api_key(api_key)
    org_id = result['org_id']

    print(f"Organization created with id {org_id}")

    return api_key, org_id


def translate_queries(api_key: str, queries: List[str], host: str | None = None) -> Tuple[int, int]:
    """
    Main entry point to translate a query end to end.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication
        queries: List of SQL queries to translate

    Returns:
        Tuple of (project_id, translation_id)
    """
    host = _get_host(host)

    # Create DMA project
    data_sources = _get_data_sources(api_key, host)
    source_data_source_id = [d for d in data_sources if d['type'] != "databricks"][0]['id']
    target_data_source_id = [d for d in data_sources if d['type'] == "databricks"][0]['id']
    print("Creating Translation Project...")
    project = _create_dma_project(api_key, source_data_source_id, target_data_source_id, 'Databricks Notebook Project', host)
    project_id = project['id']
    print(f"Translation Project created with id {project_id}.")

    # Upload queries to translate
    print("Uploading queries to translate...")
    _upload_queries(
        host=host,
        api_key=api_key,
        project_id=project_id,
        queries=queries
    )
    print("Uploaded queries to translate.")

    # Start translating queries
    translation_id = _start_translation(api_key, project_id, host)
    print(f"Started translation with id {translation_id}")
    return project_id, translation_id


def view_translation_results_as_html(api_key: str, project_id: int, translation_id: int, host: str | None = None) -> str:
    """
    View translation results

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate
        translation_id: Translation ID used to translate
    Returns:
        str: html string to be displayed in Jupyter Notebook
    """
    host = _get_host(host)
    
    print("Waiting for Translation Results...")
    translation_results = _wait_for_translation_results(api_key, project_id, translation_id, 5, host)
    print("Translation Results:")
    return _translation_results_html(translation_results)

def translate_queries_and_render_results(queries: List[str], org_token: str, host: str | None = None) -> None:
    api_key = _get_current_api_key(org_token, host)
    if api_key is None:
        raise ValueError("API key is not set. Please call create_organization or set the API key manually.")
    project_id, translation_id = translate_queries(api_key, queries, host)
    html = view_translation_results_as_html(api_key, project_id, translation_id)

    from IPython.display import HTML, display
    display(HTML(html))

def _get_data_sources(api_key: str, host: str | None = None) -> List[Dict]:
    """
    Fetch all data sources from the Datafold API.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication

    Returns:
        List of data source dictionaries
    """
    host = _get_host(host)
    url = prepare_api_url(host, "api/v1/data_sources")
    headers = prepare_headers(api_key)
    response = get_data(url, headers=headers)
    return response.json()


def _create_dma_project(api_key: str, source_ds_id: int, target_ds_id: int, name: str, host: str | None) -> Dict:
    """
    Create a DMA project.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: API token for authentication
        source_ds_id: Source data source ID
        target_ds_id: Target data source ID
        name: Project name

    Returns:
        Created project dictionary
    """
    host = _get_host(host)
    url = prepare_api_url(host, "api/internal/dma/projects")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    payload = {
        "name": name,
        "from_data_source_id": source_ds_id,
        "to_data_source_id": target_ds_id,
        "version": 2,
        "settings":  {
            "error_on_zero_diff": False,
            "transform_group_creation_strategy": "group_individual_operations",
            "experimental": {
                "import_sql_files_as_script_objects": True,
                "infer_schema_from_scripts": True,
                "generate_synthetic_data": True

            }
        }
    }

    response = post_data(url, json_data=payload, headers=headers)
    return response.json()['project']


def _upload_queries(
      api_key: str,
      project_id: int,
      queries: List[str],
      host: str = DEFAULT_HOST
  ) -> Dict:
    """
    Upload multiple queries to be translated.

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Your API authentication token
        project_id: The project ID to upload to
        queries: List of queries to upload

    Returns:
        dict: Response with upload statistics including per-file results
    """
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{project_id}/files")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    payload = {
        'files': [
            {
                "filename": f"query_{i+1}.sql",
                "content": query
            }
            for i, query in enumerate(queries)
        ]
    }
    response = post_data(url, json_data=payload, headers=headers)
    return response.json()


def _start_translation(api_key: str, project_id: int, host: str = DEFAULT_HOST) -> int:
    """
    Start translation

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate

    Returns:
        int: Translation task ID
    """
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{project_id}/translate/jobs")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    response = post_data(
        url,
        json_data={"project_id": project_id},
        headers=headers
    )
    translation_id = response.json()["task_id"]
    return translation_id


def _wait_for_translation_results(api_key: str, project_id: int, translation_id: int, poll_interval: int, host: str = DEFAULT_HOST) -> Dict:
    """
    Poll for translation completion

    Args:
        host: Host URL for Datafold instance (e.g., "https://app.datafold.com")
        api_key: Authentication token
        project_id: Project ID to translate
        translation_id: Translation ID used to translate
        poll_interval: Seconds between status checks

    Returns:
        dict: Final translation result
    """
    host = _get_host(host)
    url = prepare_api_url(host, f"api/internal/dma/v2/projects/{project_id}/translate/jobs/{translation_id}")
    headers = prepare_headers(api_key)
    headers["Content-Type"] = "application/json"

    while True:
        response = get_data(url, headers=headers)
        result = response.json()
        status = result["status"]

        if status in ["done", "failed"]:
            return result

        time.sleep(poll_interval)


def _translation_results_html(translation_results: Dict) -> str:
    """
    Generate HTML representation of translation results.

    Args:
        translation_results: Translation results dictionary

    Returns:
        str: HTML string for display
    """
    html = []
    for model in translation_results['translated_models']:
        asset_name = model['asset_name']
        html.append(f"""
        <button class="collapsible" onclick="toggleCollapse(this)">
            {asset_name}
        </button>
        <div class="content">
            {_render_translated_model_as_html(model)}
        </div>
        """)
    if not translation_results['translated_models']:
        return """No queries were translated."""

    style = """
    <style>
        .collapsible {
            background-color: #f1f1f1;
            color: #333;
            cursor: pointer;
            padding: 18px;
            width: 100%;
            border: 1px solid #ddd;
            text-align: left;
            outline: none;
            font-size: 16px;
            font-family: sans-serif;
            margin-top: 10px;
            transition: background-color 0.3s;
        }
        .collapsible:hover {
            background-color: #e0e0e0;
        }
        .collapsible.active {
            background-color: #d0d0d0;
        }
        .collapsible::before {
            content: '▶ ';
            display: inline-block;
            margin-right: 8px;
            transition: transform 0.3s;
        }
        .collapsible.active::before {
            transform: rotate(90deg);
        }
        .content {
            padding: 0 18px;
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.3s ease-out;
            background-color: white;
        }
        .content.active {
            max-height: 10000px;
            padding: 18px;
        }
    </style>
    """

    script = """
    <script>
        function toggleCollapse(element) {
            element.classList.toggle('active');
            const content = element.nextElementSibling;
            content.classList.toggle('active');
        }
    </script>
    """

    html.insert(0, ''.join([style, script]))
    return ''.join(html)


def _render_translated_model_as_html(model: Dict) -> str:
    """
    Render a single translated model as HTML with diff highlighting.

    Args:
        model: Model dictionary containing source and target SQL

    Returns:
        str: HTML string with diff visualization
    """
    source_sql = model['source_sql']
    target_sql = model['target_sql'] or ''
    status = model['translation_status']

    # Split into lines for comparison
    source_lines = source_sql.splitlines()
    target_lines = target_sql.splitlines()

    # Create a differ
    differ = difflib.Differ()
    diff = list(differ.compare(source_lines, target_lines))

    # Build HTML with highlighted differences
    source_html = []
    target_html = []

    i = 0
    while i < len(diff):
        line = diff[i]

        if line.startswith('  '):  # Unchanged line
            content = line[2:]
            source_html.append(f'<div class="line unchanged">{content}</div>')
            target_html.append(f'<div class="line unchanged">{content}</div>')
            i += 1
        elif line.startswith('- '):  # Line only in source
            content = line[2:]
            source_html.append(f'<div class="line removed">{content}</div>')
            i += 1
        elif line.startswith('+ '):  # Line only in target
            content = line[2:]
            target_html.append(f'<div class="line added">{content}</div>')
            i += 1
        elif line.startswith('? '):  # Hint line (skip)
            i += 1
        else:
            i += 1

    html = f"""
    <style>
        .sql-container {{
            display: flex;
            gap: 20px;
            font-family: monospace;
        }}
        .sql-column {{
            flex: 1;
            border: 1px solid #ddd;
            padding: 15px;
            background-color: #f5f5f5;
            overflow-x: auto;
        }}
        .sql-column h3 {{
            margin-top: 0;
            color: #333;
            font-family: sans-serif;
        }}
        .line {{
            font-size: 12px;
            line-height: 1.6;
            padding: 2px 4px;
            white-space: pre-wrap;
        }}
        .unchanged {{
            background-color: transparent;
        }}
        .removed {{
            background-color: #ffecec;
            color: #d73a49;
        }}
        .added {{
            background-color: #e6ffec;
            color: #22863a;
        }}
    </style>

    <p>Translation Status: <span class="status">{status}</span></p>
    <div class="sql-container">
        <div class="sql-column">
            <h3>Snowflake SQL</h3>
            {''.join(source_html)}
        </div>
        <div class="sql-column">
            <h3>Databricks SQL</h3>
            {''.join(target_html)}
        </div>
    </div>
    """

    return html
