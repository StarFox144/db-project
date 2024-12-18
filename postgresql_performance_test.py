import psycopg2
import random
import string
import time
from faker import Faker

class IndexedDatabasePerformanceTester:
    def __init__(self, db_params):
        """
        Initialize database connection parameters
        
        :param db_params: Dictionary with database connection parameters
        """
        self.db_params = db_params
        self.fake = Faker()

    def create_connection(self):
        """
        Create a database connection
        
        :return: Database connection
        """
        return psycopg2.connect(**self.db_params)

    def create_table_with_indexes(self):
        """
        Create a test table with indexes on commonly queried columns
        """
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                DROP TABLE IF EXISTS performance_test;
                CREATE TABLE performance_test (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    email VARCHAR(100),
                    age INT,
                    description TEXT
                );
                
                -- Create indexes on commonly queried columns
                CREATE INDEX idx_performance_test_age ON performance_test (age);
                CREATE INDEX idx_performance_test_name ON performance_test (name);
                """)
                conn.commit()

    def generate_random_data(self, num_records):
        """
        Generate random test data
        
        :param num_records: Number of records to generate
        :return: List of tuples with random data
        """
        data = []
        for _ in range(num_records):
            name = self.fake.name()
            email = self.fake.email()
            age = random.randint(18, 80)
            description = self.fake.text()
            data.append((name, email, age, description))
        return data

    def measure_insert_performance(self, num_records):
        """
        Measure insert performance for a given number of records
        
        :param num_records: Number of records to insert
        :return: Time taken for insertion
        """
        data = self.generate_random_data(num_records)
        
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                start_time = time.time()
                
                # Insert data
                cur.executemany(
                    """INSERT INTO performance_test (name, email, age, description) 
                    VALUES (%s, %s, %s, %s)""", 
                    data
                )
                
                conn.commit()
                
                end_time = time.time()
                return end_time - start_time

    def measure_select_performance(self, num_records):
        """
        Measure select performance
        
        :param num_records: Number of records to select
        :return: Time taken for selection
        """
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                start_time = time.time()
                
                # Different select scenarios
                # 1. Select by ID (primary key is already indexed)
                cur.execute("SELECT * FROM performance_test WHERE id = %s", 
                            (random.randint(1, num_records),))
                
                # 2. Select with condition on indexed age column
                cur.execute("SELECT * FROM performance_test WHERE age > %s LIMIT 100", 
                            (random.randint(18, 50),))
                
                # 3. Select with text search on indexed name column
                cur.execute("SELECT * FROM performance_test WHERE name LIKE %s", 
                            (f'%{random.choice(string.ascii_uppercase)}%',))
                
                end_time = time.time()
                return end_time - start_time

    def measure_update_performance(self, num_records):
        """
        Measure update performance
        
        :param num_records: Number of records to update
        :return: Time taken for updates
        """
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                start_time = time.time()
                
                # Update multiple scenarios
                # 1. Update by ID
                cur.execute("UPDATE performance_test SET age = %s WHERE id = %s", 
                            (random.randint(18, 80), random.randint(1, num_records)))
                
                # 2. Bulk update based on condition
                cur.execute("UPDATE performance_test SET description = %s WHERE age > %s", 
                            (self.fake.text(), random.randint(18, 50)))
                
                conn.commit()
                
                end_time = time.time()
                return end_time - start_time

    def measure_delete_performance(self, num_records):
        """
        Measure delete performance
        
        :param num_records: Number of records to delete
        :return: Time taken for deletions
        """
        with self.create_connection() as conn:
            with conn.cursor() as cur:
                start_time = time.time()
                
                # Delete scenarios
                # 1. Delete by ID
                cur.execute("DELETE FROM performance_test WHERE id = %s", 
                            (random.randint(1, num_records),))
                
                # 2. Bulk delete based on condition
                cur.execute("DELETE FROM performance_test WHERE age < %s", 
                            (random.randint(18, 40),))
                
                conn.commit()
                
                end_time = time.time()
                return end_time - start_time

    def run_performance_tests(self, record_counts):
        """
        Run performance tests for different record counts
        
        :param record_counts: List of record counts to test
        """
        results = {}
        
        for count in record_counts:
            print(f"\nTesting with {count} records (WITH INDEXES):")
            
            # Recreate table with indexes and insert data
            self.create_table_with_indexes()
            insert_time = self.measure_insert_performance(count)
            print(f"Insert Time: {insert_time:.4f} seconds")
            
            # Perform select tests
            select_time = self.measure_select_performance(count)
            print(f"Select Time: {select_time:.4f} seconds")
            
            # Perform update tests
            update_time = self.measure_update_performance(count)
            print(f"Update Time: {update_time:.4f} seconds")
            
            # Perform delete tests
            delete_time = self.measure_delete_performance(count)
            print(f"Delete Time: {delete_time:.4f} seconds")
            
            results[count] = {
                'insert': insert_time,
                'select': select_time,
                'update': update_time,
                'delete': delete_time
            }
        
        return results

def main():
    # Database connection parameters
    db_params = {
        'dbname': 'defaultdb',
        'user': 'avnadmin',
        'password': 'AVNS_O3z9kbbw2LFc1tKyV82',
        'host': 'localhost',
        'port': '5432'
    }
    
    # Record counts to test
    record_counts = [1000, 10000, 100000, 1000000]
    
    # Create performance tester
    tester = IndexedDatabasePerformanceTester(db_params)
    
    # Run performance tests
    results = tester.run_performance_tests(record_counts)
    
    # Optional: You can add code to save or visualize results here

if __name__ == '__main__':
    main()
