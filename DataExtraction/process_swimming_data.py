from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import logging
import os
from pyspark.sql.types import DoubleType
import psycopg2

class SwimmingDataProcessor:
    def __init__(self):
        """Initialize the processor with Spark session and database connection details."""
        self.spark = SparkSession.builder \
            .appName("Swimming Data Processing") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.7.2.jar") \
            .getOrCreate()
            
        # Database connection parameters
        self.db_host = "swimming-db"
        self.db_port = 5432
        self.db_user = "postgres"
        self.db_password = "postgres"
        self.db_name = "swimming_olympics"
        
        # JDBC URL for Spark
        self.jdbc_url = f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}"
        self.jdbc_properties = {
            "user": self.db_user,
            "password": self.db_password,
            "driver": "org.postgresql.Driver"
        }

        self.logger = logging.getLogger(__name__)

    def read_table(self, table_name):
        """Safely read a table from PostgreSQL"""
        try:
            return self.spark.read \
                .jdbc(url=self.jdbc_url,
                     table=table_name,
                     properties=self.jdbc_properties)
        except Exception as e:
            self.logger.warning(f"Table {table_name} not found: {str(e)}")
            return None

    def write_table(self, df, table_name, mode="overwrite"):
        """Write a DataFrame to PostgreSQL"""
        if mode == "overwrite":
            # Create a temporary table
            temp_table = f"{table_name}_temp"
            df.write \
                .jdbc(url=self.jdbc_url,
                     table=temp_table,
                     mode="overwrite",
                     properties=self.jdbc_properties)
            
            # Drop and rename in a transaction
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_password,
                database=self.db_name
            )
            conn.autocommit = False
            cursor = conn.cursor()
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
                cursor.execute(f"ALTER TABLE {temp_table} RENAME TO {table_name};")
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"Error in table swap: {str(e)}")
                raise
            finally:
                cursor.close()
                conn.close()
        else:
            df.write \
                .jdbc(url=self.jdbc_url,
                     table=table_name,
                     mode=mode,
                     properties=self.jdbc_properties)

    def drop_existing_tables(self):
        """Drop tables in reverse order of dependencies"""
        # Drop tables in reverse order of dependencies
        drop_tables_sql = """
        DROP TABLE IF EXISTS fact_rankings CASCADE;
        DROP TABLE IF EXISTS fact_swimming_performance CASCADE;
        DROP TABLE IF EXISTS dim_event CASCADE;
        DROP TABLE IF EXISTS dim_competition CASCADE;
        DROP TABLE IF EXISTS dim_swimmer CASCADE;
        """
        
        # Execute the SQL from create_tables.sql
        with open('/scripts/create_tables.sql', 'r') as f:
            create_tables_sql = f.read()
        
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Split and execute each statement separately
        for sql_stmt in drop_tables_sql.split(';'):
            if sql_stmt.strip():  # Skip empty statements
                try:
                    cursor.execute(sql_stmt)
                except Exception as e:
                    print(f"Error executing: {sql_stmt[:100]}...")
                    print(f"Error: {str(e)}")
        
        # Split and execute each statement separately
        for sql_stmt in create_tables_sql.split(';'):
            if sql_stmt.strip():  # Skip empty statements
                try:
                    cursor.execute(sql_stmt)
                except Exception as e:
                    if "database" not in str(e).lower():  # Ignore database already exists error
                        print(f"Error executing: {sql_stmt[:100]}...")
                        print(f"Error: {str(e)}")
        
        cursor.close()
        conn.close()

    def process_swimmer_dimension(self, raw_data):
        """Process swimmer dimension with SCD Type 2 support."""
        try:
            # Print raw data schema and columns
            print("Raw data schema:")
            raw_data.printSchema()
            print("\nAvailable columns:")
            print(raw_data.columns)
            
            # Data quality checks
            null_counts = raw_data.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in ["SwimmerId", "Name", "Birth_Date", "Gender"]])
            null_counts.show()
            
            # Get existing records if any
            existing_records = self.read_table("dim_swimmer")
            
            # Create new records DataFrame with all required columns
            new_records = raw_data.select(
                F.col("SwimmerId").alias("swimmer_id"),
                F.col("Name").alias("name"),
                F.col("Birth_Date").cast("date").alias("birth_date"),
                F.when(F.col("Gender") == "Women", "F")
                 .when(F.col("Gender") == "Men", "M")
                 .otherwise("U")
                 .alias("gender"),
                F.col("Citizenship").alias("nationality")
            ).distinct()
            
            if existing_records is not None:
                # Identify changed records
                changed_records = new_records.join(
                    existing_records.filter(F.col("is_current") == True),
                    (new_records.swimmer_id == existing_records.swimmer_id) &
                    (
                        (new_records.name != existing_records.name) |
                        (new_records.birth_date != existing_records.birth_date) |
                        (new_records.gender != existing_records.gender) |
                        (new_records.nationality != existing_records.nationality)
                    ),
                    "left_anti"
                )
                
                # Update valid_to and is_current for changed records
                if changed_records.count() > 0:
                    current_date = F.current_date()
                    
                    # Update existing records
                    existing_records_updated = existing_records.withColumn(
                        "valid_to",
                        F.when(
                            (F.col("is_current") == True) &
                            (F.col("swimmer_id").isin([r.swimmer_id for r in changed_records.collect()])),
                            current_date
                        ).otherwise(F.col("valid_to"))
                    ).withColumn(
                        "is_current",
                        F.when(
                            (F.col("is_current") == True) &
                            (F.col("swimmer_id").isin([r.swimmer_id for r in changed_records.collect()])),
                            False
                        ).otherwise(F.col("is_current"))
                    )
                    
                    # Prepare new versions of changed records
                    changed_records = changed_records.withColumn(
                        "valid_from", current_date
                    ).withColumn(
                        "valid_to", F.lit(None).cast("date")
                    ).withColumn(
                        "is_current", F.lit(True)
                    ).withColumn(
                        "version", F.lit(1)
                    ).withColumn(
                        "created_at", F.current_timestamp()
                    ).withColumn(
                        "updated_at", F.current_timestamp()
                    ).withColumn(
                        "swimmer_sk", F.lit(None).cast("bigint")  # Add swimmer_sk column with NULL values
                    )
                    
                    # Cast columns to match types
                    changed_records = changed_records.select(
                        F.col("swimmer_sk").cast("bigint"),
                        F.col("swimmer_id").cast("string"),
                        F.col("name").cast("string"),
                        F.col("birth_date").cast("date"),
                        F.col("gender").cast("string"),
                        F.col("nationality").cast("string"),
                        F.col("is_current").cast("boolean"),
                        F.col("valid_from").cast("date"),
                        F.col("valid_to").cast("date"),
                        F.col("version").cast("integer"),
                        F.col("created_at").cast("timestamp"),
                        F.col("updated_at").cast("timestamp")
                    )
                    
                    # Combine existing and changed records
                    final_records = existing_records_updated.union(changed_records)
                else:
                    final_records = existing_records
            else:
                # For first load, add SCD columns
                final_records = new_records.withColumn(
                    "valid_from", F.current_date()
                ).withColumn(
                    "valid_to", F.lit(None).cast("date")
                ).withColumn(
                    "is_current", F.lit(True)
                ).withColumn(
                    "version", F.lit(1)
                ).withColumn(
                    "created_at", F.current_timestamp()
                ).withColumn(
                    "updated_at", F.current_timestamp()
                )
            
            # Add surrogate key
            final_records = final_records.withColumn(
                "swimmer_sk",
                F.row_number().over(Window.orderBy("swimmer_id", "valid_from"))
            )
            
            # Write to PostgreSQL
            self.write_table(final_records, "dim_swimmer")
            
        except Exception as e:
            print(f"Error processing swimmer dimension: {str(e)}")
            raise

    def create_database(self):
        """Create the database and tables."""
        print("Creating database...")
        # Connect to default database to create swimming_olympics
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database="postgres"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Drop and create database
        try:
            # Terminate existing connections to the database
            cursor.execute("""
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = 'swimming_olympics'
                AND pid <> pg_backend_pid();
            """)
            cursor.execute("DROP DATABASE IF EXISTS swimming_olympics")
            cursor.execute("CREATE DATABASE swimming_olympics")
            print("Database created successfully")
        except Exception as e:
            print(f"Error creating database: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
            
        # Now connect to swimming_olympics and create tables
        print("Creating tables...")
        conn = psycopg2.connect(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database="swimming_olympics"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Read SQL file
        with open('create_tables.sql', 'r') as file:
            sql_commands = file.read()
        
        # Execute SQL commands
        try:
            cursor.execute(sql_commands)
            print("Tables created successfully")
        except Exception as e:
            print(f"Error creating tables: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()

    def process_swimming_data(self):
        """Process swimming data and load into database."""
        try:
            # Create database and tables first
            self.create_database()
            
            # Now proceed with data processing
            print("Starting data processing...")
            
            # Check if raw data directory exists
            if not os.path.exists('/scripts/raw_data'):
                raise Exception("Raw data directory not found at /scripts/raw_data")
            
            # Read all result files
            result_files = [f for f in os.listdir('/scripts/raw_data') if f.endswith('_results.csv')]
            if not result_files:
                raise Exception("No result files found in /scripts/raw_data")
            
            print(f"Found {len(result_files)} result files")
            
            # Data quality checks
            for file in result_files:
                file_path = os.path.join('/scripts/raw_data', file)
                df = self.spark.read.csv(file_path, header=True)
                row_count = df.count()
                null_counts = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns])
                print(f"\nFile: {file}")
                print(f"Total rows: {row_count}")
                print("Null counts per column:")
                null_counts.show()
            
            # Read and process raw data
            raw_data = self.spark.read.csv('/scripts/raw_data/*_results.csv', header=True)
            print(f"Raw data count: {raw_data.count()}")
            print("Raw data schema:")
            raw_data.printSchema()
            
            # Process dimensions and facts
            print("Processing swimmer dimension...")
            self.process_swimmer_dimension(raw_data)
            
            # Process event dimension
            print("Processing event dimension...")
            events = raw_data.select(
                F.regexp_extract(F.col("Event"), "(\d+)m", 1).cast("integer").alias("distance"),
                F.regexp_replace(F.col("Event"), "\d+m\s*", "").alias("stroke"),
                F.lit("LCM").alias("course")  # Assuming all events are in Long Course Meters
            ).distinct()
            
            # Add surrogate key
            events = events.withColumn("event_sk", F.row_number().over(Window.orderBy("distance", "stroke")))
            
            # Write event dimension
            self.write_table(events, "dim_event")
            
            # Process competition dimension
            print("Processing competition dimension...")
            competitions = raw_data.select(
                F.col("Competition_Competition_Id").alias("competition_id"),
                F.col("Competition_Date").cast("date").alias("competition_date"),
                F.col("Competition_Country").alias("location_code"),
                F.col("Competition_Level").alias("competition_name")
            ).distinct()
            
            # Add surrogate key
            competitions = competitions.withColumn("competition_sk", F.row_number().over(Window.orderBy("competition_id")))
            
            # Write competition dimension
            self.write_table(competitions, "dim_competition")
            
            # Process fact swimming performance
            print("Processing fact swimming performance...")
            # Join with dimensions to get surrogate keys
            swimmers_df = self.spark.read.jdbc(url=self.jdbc_url, table="dim_swimmer", properties=self.jdbc_properties)
            events_df = self.spark.read.jdbc(url=self.jdbc_url, table="dim_event", properties=self.jdbc_properties)
            competitions_df = self.spark.read.jdbc(url=self.jdbc_url, table="dim_competition", properties=self.jdbc_properties)
            
            # Convert time to seconds
            def time_to_seconds(time_str):
                try:
                    if not time_str:
                        return None
                    parts = time_str.split(":")
                    if len(parts) == 3:  # HH:MM:SS.ss
                        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
                    elif len(parts) == 2:  # MM:SS.ss
                        return float(parts[0]) * 60 + float(parts[1])
                    else:  # SS.ss
                        return float(parts[0])
                except:
                    return None
            
            time_to_seconds_udf = F.udf(time_to_seconds, DoubleType())
            
            # Create fact performance DataFrame
            fact_performance = raw_data \
                .join(swimmers_df, raw_data.SwimmerId == swimmers_df.swimmer_id) \
                .join(events_df, 
                      (F.regexp_extract(raw_data.Event, "(\d+)m", 1).cast("integer") == events_df.distance) & 
                      (F.regexp_replace(raw_data.Event, "\d+m\s*", "") == events_df.stroke)) \
                .join(competitions_df, raw_data.Competition_Competition_Id == competitions_df.competition_id) \
                .select(
                    F.row_number().over(Window.orderBy(F.lit(1))).alias("performance_id"),
                    swimmers_df.swimmer_sk.alias("swimmer_sk"),
                    events_df.event_sk.alias("event_sk"),
                    competitions_df.competition_sk.alias("competition_sk"),
                    time_to_seconds_udf(F.col("Time")).alias("time_seconds"),
                    F.col("Overall_Rank").cast("integer").alias("overall_rank"),
                    F.col("Age").cast("integer").alias("age")
                )
            
            # Write fact performance
            print("Writing fact swimming performance...")
            self.write_table(fact_performance, "fact_swimming_performance", mode="append")
            
            # Process fact rankings
            print("Processing fact rankings...")
            # Window for finding best time and rank per swimmer and event
            window_spec = Window.partitionBy('swimmer_sk', 'event_sk')
            
            fact_rankings = fact_performance \
                .groupBy('swimmer_sk', 'event_sk') \
                .agg(
                    F.min('time_seconds').alias('best_time'),
                    F.min('overall_rank').alias('best_rank')
                )
            
            # Write fact rankings
            print("Writing fact rankings...")
            self.write_table(fact_rankings, "fact_rankings", mode="append")
            
            print("Data processing completed successfully!")
            
        except Exception as e:
            print(f"Error in data processing: {str(e)}")
            raise
    
if __name__ == "__main__":
    processor = SwimmingDataProcessor()
    processor.process_swimming_data()