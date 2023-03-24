import pytest
from .context import prepare_postgres

# Before all in session
@pytest.fixture(scope='session', autouse=True)
def db_setup():
    # Run prepare database
    db = prepare_postgres.prep_db("../../postgres_connection.json", "../../tpch-dbgen")
    db.prepare_test_database("test_data", "../../tpch-prep")
    print("Database prepared")


# After all
@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""
    def cleanup_db():
        # Cleanup database
        db = prepare_postgres.prep_db("../../postgres_connection.json", "../../tpch-dbgen")
        db.clean_database()
        
    request.addfinalizer(cleanup_db)
    