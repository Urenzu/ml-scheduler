"""
Data layer: Parquet I/O and PostgreSQL metadata management
"""
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


class DataLayer:
    """Manages dataset storage (Parquet) and metadata (PostgreSQL)"""
    
    def __init__(self, db_url: str, data_dir: str = "./data"):
        self.db_url = db_url
        self.data_dir = Path(data_dir)
        self.engine = create_engine(db_url, poolclass=NullPool)
        
        # Create directory structure
        for layer in ['raw', 'processed', 'curated']:
            (self.data_dir / layer).mkdir(parents=True, exist_ok=True)
    
    def write_dataset(
        self,
        df: pd.DataFrame,
        name: str,
        layer: str = 'raw',
        metadata: Optional[Dict[str, Any]] = None,
        version: Optional[int] = None
    ) -> int:
        """
        Write dataset to Parquet and register in PostgreSQL
        
        Returns: dataset_id
        """
        # Determine version
        if version is None:
            version = self._get_next_version(name, layer)
        
        # Generate file path
        filename = f"{name}_v{version}.parquet"
        file_path = self.data_dir / layer / filename
        
        # Write Parquet file with compression
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table,
            file_path,
            compression='snappy',
            use_dictionary=True
        )
        
        # Extract schema
        schema_json = {
            field.name: str(field.type)
            for field in table.schema
        }
        
        # Get file size
        size_bytes = file_path.stat().st_size
        
        # Register in PostgreSQL
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    INSERT INTO datasets (name, version, layer, file_path, schema_json, 
                                        row_count, size_bytes, metadata)
                    VALUES (:name, :version, :layer, :file_path, :schema_json,
                            :row_count, :size_bytes, :metadata)
                    RETURNING id
                """),
                {
                    'name': name,
                    'version': version,
                    'layer': layer,
                    'file_path': str(file_path),
                    'schema_json': json.dumps(schema_json),
                    'row_count': len(df),
                    'size_bytes': size_bytes,
                    'metadata': json.dumps(metadata or {})
                }
            )
            conn.commit()
            dataset_id = result.fetchone()[0]
        
        print(f"Dataset written: {name} v{version} ({layer}) - {len(df)} rows, {size_bytes/1024/1024:.2f} MB")
        return dataset_id
    
    def read_dataset(
        self,
        name: str,
        layer: str = 'raw',
        version: Optional[int] = None
    ) -> pd.DataFrame:
        """Read dataset from Parquet"""
        if version is None:
            version = self._get_latest_version(name, layer)
        
        filename = f"{name}_v{version}.parquet"
        file_path = self.data_dir / layer / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path}")
        
        return pd.read_parquet(file_path)
    
    def get_dataset_metadata(self, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Retrieve dataset metadata from PostgreSQL"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM datasets WHERE id = :id"),
                {'id': dataset_id}
            )
            row = result.fetchone()
            
            if row is None:
                return None
            
            return {
                'id': row[0],
                'name': row[1],
                'version': row[2],
                'layer': row[3],
                'file_path': row[4],
                'schema': json.loads(row[5]) if row[5] else {},
                'row_count': row[6],
                'size_bytes': row[7],
                'created_at': row[8],
                'metadata': json.loads(row[9]) if row[9] else {}
            }
    
    def list_datasets(self, layer: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all datasets, optionally filtered by layer"""
        query = "SELECT id, name, version, layer, row_count, size_bytes, created_at FROM datasets"
        params = {}
        
        if layer:
            query += " WHERE layer = :layer"
            params['layer'] = layer
        
        query += " ORDER BY created_at DESC"
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query), params)
            return [
                {
                    'id': row[0],
                    'name': row[1],
                    'version': row[2],
                    'layer': row[3],
                    'row_count': row[4],
                    'size_bytes': row[5],
                    'created_at': row[6]
                }
                for row in result
            ]
    
    def _get_next_version(self, name: str, layer: str) -> int:
        """Get next version number for a dataset"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT COALESCE(MAX(version), 0) + 1
                    FROM datasets
                    WHERE name = :name AND layer = :layer
                """),
                {'name': name, 'layer': layer}
            )
            return result.fetchone()[0]
    
    def _get_latest_version(self, name: str, layer: str) -> int:
        """Get latest version number for a dataset"""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT MAX(version)
                    FROM datasets
                    WHERE name = :name AND layer = :layer
                """),
                {'name': name, 'layer': layer}
            )
            version = result.fetchone()[0]
            if version is None:
                raise ValueError(f"No dataset found: {name} ({layer})")
            return version