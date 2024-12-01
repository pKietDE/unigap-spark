import logging

class ProductWriteProcessor:
    # Cấu hình logging
    logging.basicConfig(level=logging.INFO)
    
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ProductWriteProcessor, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, 'postgres_options'):
            self.postgres_options = {
                "url": "jdbc:postgresql://34.29.192.39:5432/glamira",
                "user": "postgres",
                "password": "UnigapPostgres@123",
                "driver": "org.postgresql.Driver"
            }

    
    def write_postgres(self, df, db_table, schema="public"):
        try:
            logging.info(f"Attempting to append data to table {schema}.{db_table}")
            df.write \
                .format("jdbc") \
                .options(**self.postgres_options) \
                .option("dbtable", f"{schema}.{db_table}") \
                .mode("append") \
                .save()
            logging.info(f"Data successfully appended to {schema}.{db_table}")
        except Exception as e:
            logging.error(f"Failed to append data to {schema}.{db_table}: {e}")
            logging.info("Attempting to overwrite data...")
            try:
                df.write \
                    .format("jdbc") \
                    .options(**self.postgres_options) \
                    .option("dbtable", f"{schema}.{db_table}") \
                    .mode("overwrite") \
                    .save()
                logging.info(f"Data successfully overwritten in {schema}.{db_table}")
            except Exception as e:
                logging.error(f"Failed to overwrite data in {schema}.{db_table}: {e}")
