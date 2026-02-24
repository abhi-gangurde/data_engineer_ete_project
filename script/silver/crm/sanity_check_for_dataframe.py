from pyspark.sql import functions as F
from pyspark.sql.types import StructType

def check_df_sanity(df, expected_schema=None, pk_cols=None, critical_cols=None):
    """
    Performs essential sanity checks on a PySpark DataFrame.
    
    :param df: The Spark DataFrame to check.
    :param expected_schema: (Optional) A StructType to validate against.
    :param pk_cols: (Optional) List of columns that must be unique (Primary Keys).
    :param critical_cols: (Optional) List of columns that should not have NULL values.
    """
    print("--- Starting Sanity Check ---")
    
    # 1. Row Count Check
    # Using limit(1).count() is faster than count() for checking if data exists
    if df.limit(1).count() == 0:
        print("❌ ERROR: DataFrame is EMPTY.")
        return False
    row_count = df.count()
    print(f"✅ Row Count: {row_count}")

    # 2. Schema Validation
    if expected_schema:
        if df.schema == expected_schema:
            print("✅ Schema: Matched expected structure.")
        else:
            print("❌ ERROR: Schema MISMATCH detected.")
            df.printSchema()
            return False

    # 3. Primary Key Uniqueness
    if pk_cols:
        distinct_count = df.select(pk_cols).distinct().count()
        if distinct_count == row_count:
            print(f"✅ Uniqueness: No duplicates in PK columns {pk_cols}.")
        else:
            print(f"❌ ERROR: Found {row_count - distinct_count} duplicates in PK columns.")
            return False

    # 4. Critical Column Null Checks
    if critical_cols:
        null_counts = df.select([
            F.count(F.when(F.col(c).isNull(), c)).alias(c) 
            for c in critical_cols
        ]).collect()[0].asDict()
        
        has_nulls = False
        for col, count in null_counts.items():
            if count > 0:
                print(f"❌ ERROR: Column '{col}' has {count} NULL values.")
                has_nulls = True
        if not has_nulls:
            print(f"✅ Nulls: No NULL values in critical columns {critical_cols}.")
        else:
            return False

    print("--- Sanity Check PASSED ---")
    return True

# --- Usage Example ---
# sanity_passed = check_df_sanity(df, pk_cols=["user_id"], critical_cols=["email"])
