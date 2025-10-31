"""
Data ingestion: Simulated streaming and batch ingestion
"""
import time
import random
from typing import Generator, Dict, Any
from datetime import datetime, timedelta

import pandas as pd


class DataIngestionSimulator:
    """Simulates streaming and batch data ingestion"""
    
    def __init__(self, seed: int = 42):
        random.seed(seed)
    
    def generate_research_data_batch(
        self,
        num_records: int = 1000,
        dataset_type: str = 'text'
    ) -> pd.DataFrame:
        """
        Generate a batch of simulated research data
        
        dataset_type: 'text', 'numerical', 'mixed'
        """
        if dataset_type == 'text':
            return self._generate_text_data(num_records)
        elif dataset_type == 'numerical':
            return self._generate_numerical_data(num_records)
        else:
            return self._generate_mixed_data(num_records)
    
    def stream_research_data(
        self,
        batch_size: int = 100,
        num_batches: int = 10,
        delay_seconds: float = 1.0
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Simulate streaming data ingestion
        
        Yields batches of data with delays
        """
        for i in range(num_batches):
            batch = self.generate_research_data_batch(batch_size)
            yield batch
            
            if i < num_batches - 1:
                time.sleep(delay_seconds)
    
    def _generate_text_data(self, num_records: int) -> pd.DataFrame:
        """Generate text dataset (e.g., research papers, documents)"""
        
        topics = ['ML', 'NLP', 'Computer Vision', 'Robotics', 'Theory']
        authors = [f'Author_{i}' for i in range(50)]
        
        data = []
        for i in range(num_records):
            record = {
                'id': i,
                'title': f'Research Paper {i}: {random.choice(topics)}',
                'abstract': ' '.join([f'word{random.randint(1, 1000)}' for _ in range(50)]),
                'author': random.choice(authors),
                'topic': random.choice(topics),
                'citation_count': random.randint(0, 500),
                'publication_date': self._random_date(),
                'word_count': random.randint(3000, 10000),
                'ingestion_timestamp': datetime.now()
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_numerical_data(self, num_records: int) -> pd.DataFrame:
        """Generate numerical dataset (e.g., sensor data, experiments)"""
        
        data = []
        for i in range(num_records):
            record = {
                'id': i,
                'experiment_id': f'exp_{random.randint(1, 100)}',
                'measurement_1': random.gauss(100, 15),
                'measurement_2': random.gauss(50, 10),
                'measurement_3': random.uniform(0, 1),
                'temperature': random.uniform(20, 30),
                'pressure': random.uniform(990, 1020),
                'timestamp': self._random_timestamp(),
                'quality_score': random.uniform(0.7, 1.0),
                'ingestion_timestamp': datetime.now()
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _generate_mixed_data(self, num_records: int) -> pd.DataFrame:
        """Generate mixed dataset"""
        
        categories = ['A', 'B', 'C', 'D']
        
        data = []
        for i in range(num_records):
            record = {
                'id': i,
                'name': f'Sample_{i}',
                'category': random.choice(categories),
                'value': random.gauss(100, 20),
                'count': random.randint(1, 1000),
                'flag': random.choice([True, False]),
                'description': f'Description for sample {i}',
                'created_at': self._random_timestamp(),
                'ingestion_timestamp': datetime.now()
            }
            data.append(record)
        
        return pd.DataFrame(data)
    
    def _random_date(self) -> str:
        """Generate random date string"""
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2024, 12, 31)
        random_date = start_date + timedelta(
            days=random.randint(0, (end_date - start_date).days)
        )
        return random_date.strftime('%Y-%m-%d')
    
    def _random_timestamp(self) -> datetime:
        """Generate random timestamp"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime.now()
        return start_date + timedelta(
            seconds=random.randint(0, int((end_date - start_date).total_seconds()))
        )


def ingest_csv_file(file_path: str) -> pd.DataFrame:
    """Ingest data from CSV file"""
    return pd.read_csv(file_path)


def ingest_json_file(file_path: str) -> pd.DataFrame:
    """Ingest data from JSON file"""
    return pd.read_json(file_path)