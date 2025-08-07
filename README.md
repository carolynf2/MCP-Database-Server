# MCP Database Server

**MCP Database Server** is a Python 3 application that provides a unified interface for handling local database operations across multiple database backends, including SQLite, PostgreSQL, MySQL, and MongoDB. It optionally integrates with Redis for caching query results.

## Features

- **Unified API for Multiple Databases:**  
  Supports SQLite (always enabled), and, if drivers are installed, also supports PostgreSQL, MySQL (via `pymysql`), and MongoDB.

- **Pluggable Caching:**  
  Integrates with Redis for caching query results to speed up repeated queries.

- **Automatic Configuration:**  
  Loads sensible defaults but allows full custom configuration.

- **Extensible Architecture:**  
  Easily extendable to support additional databases or custom operations.

- **Logging:**  
  Built-in logging for all operations and error handling.

## Supported Databases & Requirements

- **SQLite:** Standard library, always enabled.
- **PostgreSQL:** Requires [`psycopg2`](https://pypi.org/project/psycopg2/)
- **MySQL:** Requires [`pymysql`](https://pypi.org/project/pymysql/)
- **MongoDB:** Requires [`pymongo`](https://pypi.org/project/pymongo/)
- **Redis (optional, for caching):** Requires [`redis`](https://pypi.org/project/redis/)

Install optional dependencies as needed:
```bash
pip install psycopg2 pymysql pymongo redis
```

## Usage

### Running the Server

Run the script directly:
```bash
python mcp_database_server.py
```

This will execute the `main()` function, which provides a sample usage with a SQLite query.

### Example Request

A client request is a Python dictionary with the following structure:
```python
{
    "db_type": "sqlite",           # One of: 'sqlite', 'postgresql', 'mysql', 'mongodb'
    "operation": "select",         # Database operation, e.g., 'select', 'insert', etc.
    "query": "SELECT * FROM users WHERE id = ?",  # SQL or Mongo query
    "parameters": [1],             # Parameters for the query (dict for MongoDB)
    "cache_key": "user_1"          # Optional: cache key for Redis
}
```

### Example Output

```json
{
  "status": "success",
  "data": [
    {"name": "users"},
    {"name": "orders"}
  ],
  "from_cache": false,
  "timestamp": "1620000000.0"
}
```

### Custom Configuration

You can pass a custom configuration dictionary to `MCPDatabaseServer(config=...)` to override database settings, credentials, or enable/disable specific backends.

### Error Handling

If an error occurs, the response will have:
```json
{
  "status": "error",
  "error": "Error message here",
  "timestamp": "1620000000.0"
}
```

## Extending

- Add new database types by implementing a new handler (`_handle_newdb`) and updating `_route_request`.
- Add pre/post-processing logic as needed.

## Security Warning

**Do not expose this server directly to untrusted clients!**  
This program is intended for local/server-side use and assumes trusted input. If exposed to a network or untrusted users, it must be properly sandboxed and validated to avoid SQL injection and other security risks.

## License

[MIT](LICENSE) (or specify your own license)
