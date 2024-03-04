import unittest
from unittest.mock import MagicMock, patch

def connect_to_postgres(dbname, user, password, host, port):
    # Simulate connection to PostgreSQL
    return MagicMock()

def load_table(conn, table_name, data):
    # Simulate loading data into a PostgreSQL table
    pass

class TestPostgresUtils(unittest.TestCase):

    @patch('__main__.connect_to_postgres')
    def test_connect_to_postgres(self, mock_connect):
        # Mock connect_to_postgres function
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        # Call connect_to_postgres
        conn = connect_to_postgres('testdb', 'consultants', 'WelcomeItc@2022', 'ec2-3-9-191-104.eu-west-2.compute.amazonaws.com', '5432')

        # Assertions
        mock_connect.assert_called_once_with(
            'testdb', 'consultants', 'WelcomeItc@2022', 'ec2-3-9-191-104.eu-west-2.compute.amazonaws.com', '5432'
        )
        self.assertEqual(conn, mock_conn)

    @patch('__main__.load_table')
    def test_load_table(self, mock_load_table):
        # Mock load_table function
        mock_conn = MagicMock()
        mock_load_table.return_value = True

        # Call load_table
        table_name = 'anu_sqp1'
        data = [(1, 'John'), (2, 'Alice'), (3, 'Bob')]
        result = load_table(mock_conn, table_name, data)

        # Assertions
        mock_load_table.assert_called_once_with(
            mock_conn, table_name, data
        )
        self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()
