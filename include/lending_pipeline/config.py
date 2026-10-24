"""Central place for paths and settings used by every pipeline step."""
import os
from pathlib import Path

# include/ folder. Astro mounts it into the Airflow containers at
# /usr/local/airflow/include, so the same relative paths work locally and in Docker.
INCLUDE_DIR = Path(__file__).resolve().parents[1]

# Public Prosper Marketplace loan dataset (113,937 loans, 2005-2014). No API key needed.
SOURCE_URL = os.getenv(
    "LOANS_SOURCE_URL",
    "https://s3.amazonaws.com/udacity-hosted-downloads/ud651/prosperLoanData.csv",
)

RAW_CSV = INCLUDE_DIR / "data" / "raw" / "prosperLoanData.csv"
SEEDS_DIR = INCLUDE_DIR / "seeds"
SQL_DIR = INCLUDE_DIR / "sql"
EXPORT_DIR = INCLUDE_DIR / "exports"
WAREHOUSE_PATH = Path(os.getenv("WAREHOUSE_PATH", INCLUDE_DIR / "warehouse" / "lending.duckdb"))

# Columns the rest of the pipeline depends on. Extract fails fast if the source drops one.
REQUIRED_COLUMNS = [
    "ListingKey", "LoanKey", "MemberKey", "ListingCreationDate", "LoanOriginationDate",
    "LoanOriginalAmount", "BorrowerRate", "BorrowerAPR", "Term", "LoanStatus",
    "ProsperRating (Alpha)", "CreditGrade", "ProsperScore", "ListingCategory (numeric)",
    "BorrowerState", "IncomeRange", "EmploymentStatus", "IsBorrowerHomeowner",
    "LP_CustomerPayments", "LP_NetPrincipalLoss",
]
