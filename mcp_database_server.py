#!/usr/bin/env python3
"""
MCP Database Server - Local Database Integration
Translates pseudocode workflow into Python implementation for local database access.
"""

import json
import sqlite3
import logging
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass
import os
from pathlib import Path

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import psycopg2
    try:
        from psycopg2.extras import RealDictCursor
    except ImportError:
        RealDictCursor = None
    POSTGRESQL_AVAILABLE = True
except ImportError:
    POSTGRESQL_AVAILABLE = False
    RealDictCursor = None

try:
    import pymysql  # Use pymysql as the default MySQL driver
    MYSQL_DRIVER = 'pymysql'
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_DRIVER = None
    MYSQL_AVAILABLE = False

try:
    import pymongo
    MONGODB_AVAILABLE = True
except ImportError:
    MONGODB_AVAILABLE = False

@dataclass
class DatabaseRequest:
    """Represents a client request to the MCP server"""
    db_type: str
    operation: str
    query: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    cache_key: Optional[str] = None

class MCPDatabaseServer:
    """Main MCP Database Server handling local database operations"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or self._load_default_config()
        self.logger = self._setup_logging()
        self.redis_client = None
        
        if REDIS_AVAILABLE and self.config.get('redis', {}).get('enabled', False):
            try:
                self.redis_client = redis.Redis(
                    host=self.config['redis'].get('host', 'localhost'),
                    port=self.config['redis'].get('port', 6379),
                    decode_responses=True
                )
                self.redis_client.ping()
                self.logger.info("Redis cache connected")
            except Exception as e:
                self.logger.warning(f"Redis connection failed: {e}")
                self.redis_client = None
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration for local database connections"""
        return {
            'sqlite': {
                'enabled': True,
                'db_path': './data/app.db'
            },
            'postgresql': {
                'enabled': POSTGRESQL_AVAILABLE,
                'host': 'localhost',
                'port': 5432,
                'database': 'mcp_db',
                'user': 'postgres',
                'password': 'password'
            },
            'mysql': {
                'enabled': MYSQL_AVAILABLE,
                'host': 'localhost',
                'port': 3306,
                'database': 'mcp_db',
                'user': 'root',
                'password': 'password'
            },
            'mongodb': {
                'enabled': MONGODB_AVAILABLE,
                'host': 'localhost',
                'port': 27017,
                'database': 'mcp_db'
            },
            'redis': {
                'enabled': REDIS_AVAILABLE,
                'host': 'localhost',
                'port': 6379,
                'ttl': 3600
            }
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('mcp_database_server')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def receive_client_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point - receives and processes client requests
        Following the pseudocode workflow
        """
        try:
            # Parse request
            request = self._parse_request(request_data)
            self.logger.info(f"Processing {request.db_type} request: {request.operation}")
            
            # Check Redis cache first (if enabled)
            if request.cache_key and self.redis_client:
                cached_result = self._check_cache(request.cache_key)
                if cached_result:
                    self.logger.info("Cache hit - returning cached result")
                    return self._format_response(cached_result, from_cache=True)
            
            # Route to appropriate database handler
            result = self._route_request(request)
            
            # Store in cache if configured
            if request.cache_key and self.redis_client and result:
                self._store_in_cache(request.cache_key, result)
            
            # Format and return response
            return self._format_response(result)
            
        except Exception as e:
            self.logger.error(f"Request processing failed: {e}")
            return self._format_error_response(str(e))
    
    def _parse_request(self, request_data: Dict[str, Any]) -> DatabaseRequest:
        """Parse incoming request data into structured format"""
        return DatabaseRequest(
            db_type=request_data.get('db_type', '').lower(),
            operation=request_data.get('operation', ''),
            query=request_data.get('query'),
            parameters=request_data.get('parameters', {}),
            cache_key=request_data.get('cache_key')
        )
    
    def _route_request(self, request: DatabaseRequest) -> Any:
        """Route request to appropriate database handler based on db_type"""
        if request.db_type == "sqlite":
            return self._handle_sqlite(request)
        elif request.db_type == "postgresql":
            return self._handle_postgresql(request)
        elif request.db_type == "mysql":
            return self._handle_mysql(request)
        elif request.db_type == "mongodb":
            return self._handle_mongodb(request)
        else:
            raise ValueError(f"Unsupported database type: {request.db_type}")
    
    def _handle_sqlite(self, request: DatabaseRequest) -> Any:
        """Handle SQLite database operations"""
        if not self.config['sqlite']['enabled']:
            raise RuntimeError("SQLite not enabled")
        
        db_path = self.config['sqlite']['db_path']
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            cursor = conn.cursor()
            
            if request.operation.lower() == 'select' or request.query.lower().startswith('select'):
                cursor.execute(request.query, request.parameters or [])
                result = [dict(row) for row in cursor.fetchall()]
            else:
                cursor.execute(request.query, request.parameters or [])
                conn.commit()
                result = {"affected_rows": cursor.rowcount, "lastrowid": cursor.lastrowid}
            
            return result
    
    def _handle_postgresql(self, request: DatabaseRequest) -> Any:
        """Handle PostgreSQL database operations"""
        if not POSTGRESQL_AVAILABLE or not self.config['postgresql']['enabled']:
            raise RuntimeError("PostgreSQL not available or not enabled")
        if 'psycopg2' not in globals() or psycopg2 is None:
            raise RuntimeError("psycopg2 library is not available")
        if RealDictCursor is None:
            raise RuntimeError("psycopg2.extras.RealDictCursor is not available. Please install the correct psycopg2 version.")
        config = self.config['postgresql']
        conn = psycopg2.connect(
            host=config['host'],
            port=config['port'],
            database=config['database'],
            user=config['user'],
            password=config['password']
        )
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(request.query, request.parameters)
                if request.operation.lower() == 'select' or request.query.lower().startswith('select'):
                    result = [dict(row) for row in cursor.fetchall()]
                else:
                    conn.commit()
                    result = {"affected_rows": cursor.rowcount}
                return result
        finally:
            conn.close()
    def _handle_mysql(self, request: DatabaseRequest) -> Any:
        """Handle MySQL database operations"""
        if not MYSQL_AVAILABLE or not self.config['mysql']['enabled']:
            raise RuntimeError("MySQL not available or not enabled")
        config = self.config['mysql']
        # Only pymysql is supported in import logic above
        if MYSQL_DRIVER == 'pymysql':
            try:
                import pymysql
            except ImportError:
                raise RuntimeError("pymysql is not installed but required for MySQL support.")
            conn = pymysql.connect(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database']
            )
        else:
            raise RuntimeError("No suitable MySQL driver found. Only 'pymysql' is supported in this configuration.")
        try:
            with conn.cursor() as cursor:
                cursor.execute(request.query, request.parameters)
                if request.operation.lower() == 'select' or request.query.lower().startswith('select'):
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    result = [dict(zip(columns, row)) for row in rows]
                else:
                    conn.commit()
                    result = {"affected_rows": cursor.rowcount}
                return result
        finally:
            conn.close()
    
    def _handle_mongodb(self, request: DatabaseRequest) -> Any:
        """Handle MongoDB database operations"""
        if not MONGODB_AVAILABLE or not self.config['mongodb']['enabled']:
            raise RuntimeError("MongoDB not available or not enabled")
        if 'pymongo' not in globals() or pymongo is None:
            raise RuntimeError("pymongo library is not available")
        config = self.config['mongodb']
        client = pymongo.MongoClient(f"mongodb://{config['host']}:{config['port']}")
        try:
            db = client[config['database']]
            # Simple operation routing for MongoDB
            if request.operation == 'find':
                collection = db[request.parameters.get('collection', 'default')]
                query = request.parameters.get('query', {})
                result = list(collection.find(query))
                # Convert ObjectId to string for JSON serialization
                for doc in result:
                    if '_id' in doc:
                        doc['_id'] = str(doc['_id'])
                return result
            elif request.operation == 'insert':
                collection = db[request.parameters.get('collection', 'default')]
                document = request.parameters.get('document', {})
                result = collection.insert_one(document)
                return {"inserted_id": str(result.inserted_id)}
            elif request.operation == 'update':
                collection = db[request.parameters.get('collection', 'default')]
                query = request.parameters.get('query', {})
                update = request.parameters.get('update', {})
                result = collection.update_many(query, update)
                return {"matched_count": result.matched_count, "modified_count": result.modified_count}
            elif request.operation == 'delete':
                collection = db[request.parameters.get('collection', 'default')]
                query = request.parameters.get('query', {})
                result = collection.delete_many(query)
                return {"deleted_count": result.deleted_count}
            else:
                raise ValueError(f"Unsupported MongoDB operation: {request.operation}")
        finally:
            client.close()
    
    def _check_cache(self, cache_key: str) -> Optional[Any]:
        """Check Redis cache for cached result"""
        if not self.redis_client:
            return None
        
        try:
            cached_data = self.redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Cache read failed: {e}")
        
        return None
    
    def _store_in_cache(self, cache_key: str, data: Any) -> None:
        """Store result in Redis cache with TTL"""
        if not self.redis_client:
            return
        
        try:
            ttl = self.config['redis'].get('ttl', 3600)
            self.redis_client.setex(
                cache_key,
                ttl,
                json.dumps(data, default=str)  # default=str handles datetime, ObjectId, etc.
            )
            self.logger.debug(f"Data cached with key: {cache_key}")
        except Exception as e:
            self.logger.warning(f"Cache write failed: {e}")
    
    def _format_response(self, data: Any, from_cache: bool = False) -> Dict[str, Any]:
        """Format successful response"""
        return {
            "status": "success",
            "data": data,
            "from_cache": from_cache,
            "timestamp": str(Path(__file__).stat().st_mtime)
        }
    
    def _format_error_response(self, error_message: str) -> Dict[str, Any]:
        """Format error response"""
        return {
            "status": "error",
            "error": error_message,
            "timestamp": str(Path(__file__).stat().st_mtime)
        }

def main():
    """Example usage of the MCP Database Server"""
    server = MCPDatabaseServer()
    
    # Example SQLite request
    sqlite_request = {
        "db_type": "sqlite",
        "operation": "select",
        "query": "SELECT name FROM sqlite_master WHERE type='table'",
        "cache_key": "sqlite_tables"
    }
    
    response = server.receive_client_request(sqlite_request)
    print("SQLite Response:", json.dumps(response, indent=2))

if __name__ == "__main__":
    main()