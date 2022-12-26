import unittest

from src.fetch_data import fetch_data
from src.process_data import process_data
from src.transform_data import transform_data
from src.store_data import store_data

class TestDataPipeline(unittest.TestCase):
    def test_fetch_data(self):
        # Test that fetch_data returns a list of file names
        file_names = fetch_data()
        self.assertIsInstance(file_names, list)
        
        # Test that the list is not empty
        self.assertGreater(len(file_names), 0)
        
        # Test that all items in the list are strings
        for file_name in file_names:
            self.assertIsInstance(file_name, str)
    
    def test_process_data(self):
        # Test that process_data returns a list of dictionaries
        data = process_data('some_file.json')
        self.assertIsInstance(data, list)
        
        # Test that the list is not empty
        self.assertGreater(len(data), 0)
        
        # Test that all items in the list are dictionaries
        for item in data:
            self.assertIsInstance(item, dict)
    
    def test_transform_data(self):
        # Test that transform_data returns a DataFrame
        df = transform_data([{'some': 'data'}])
        self.assertIsInstance(df, pd.DataFrame)
        
        # Test that the DataFrame is not empty
        self.assertGreater(len(df), 0)
    
    def test_store_data(self):
        # Test that store_data inserts rows into the database
        # First, get the current number of rows in the table
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) FROM events')
        num_rows = cur.fetchone()[0]
        
        # Insert some data
        df = pd.DataFrame([{'event': 'create', 'on': 'some_entity'}])
        store_data(df)
        
        # Check that the number of rows has increased by one
        cur.execute('SELECT COUNT(*) FROM events')
        self.assertEqual(cur.fetchone()[0], num_rows + 1)

if __name__ == '__main__':
    unittest.main()