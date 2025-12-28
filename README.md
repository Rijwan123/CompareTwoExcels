# Excel Comparison Tool (Data + Structure Validation)

A high-performance Python utility to compare two Excel files and detect **data mismatches** as well as **structural changes** such as column add, remove, and reorder.  
Designed to efficiently handle large Excel files (50k+ rows) with clear, audit-friendly output.

---

## 📌 Key Features

- Compares **large Excel files** (50,000+ rows supported)
- Validates **both data and structure**
  - Cell-level data mismatches
  - Column added / removed
  - Column order changed
- Intelligent **data normalization**
  - Trims extra spaces
  - Collapses multiple whitespaces
  - Normalizes dates and timestamps
  - Handles null and empty values safely
- Supports **row alignment using a key column** (recommended) or **row order**
- Uses **chunking and multithreading** for faster execution
- Avoids false positives by comparing only **common columns**
- Generates **clear CSV and JSON reports**
- Suitable for **POC, regression testing, audits, and automation pipelines**

---

## 🛠 Tools & Technologies Used

- **Python 3.9+**
- **Pandas** – Excel reading and data processing
- **NumPy** – Efficient numeric handling
- **OpenPyXL** – Reading `.xlsx` files
- **ThreadPoolExecutor** – Parallel processing
- **CSV / JSON** – Reporting outputs

---

## ⚙️ Techniques Implemented

- **Data Normalization**
  - Removes formatting differences before comparison
  - Prevents false mismatches caused by spaces, date formats, or null values
- **Schema / Structure Validation**
  - Detects missing columns
  - Detects newly added columns
  - Detects column order changes
- **Chunk-based Processing**
  - Splits large datasets into smaller chunks
  - Reduces memory usage
- **Multithreading**
  - Compares chunks in parallel
  - Significantly improves performance on large files
- **Key-based Row Alignment**
  - Ensures accurate comparison even when row order differs

---

## 📂 Output Files

The tool generates the following outputs:

### 1️⃣ `mismatches_<timestamp>.csv`
Contains **both structure and data mismatches**:

- **STRUCTURE**
  - Column removed
  - Column added
  - Column reordered
- **DATA**
  - Actual value differences between files

### 2️⃣ `missing_in_file1.csv`
- Rows present in target file but missing in reference file

### 3️⃣ `missing_in_file2.csv`
- Rows present in reference file but missing in target file

### 4️⃣ `summary.json`
- Overall PASS / FAIL status
- Comparison statistics
- Structure validation details
- Output file references

---

## 🚀 How to Run the Code

1️⃣ Prerequisites

Install dependencies:
```bash
pip install pandas numpy openpyxl

2️⃣ Create a Virtual Environment (Recommended)
Windows
python -m venv env
.\env\Scripts\activate

Linux / macOS
python3 -m venv env
source env/bin/activate


You should see (env) in the terminal.

3️⃣ Install Required Dependencies
pip install pandas numpy openpyxl


Or, if requirements.txt exists:

pip install -r requirements.txt

4️⃣ Prepare Input Files

Ensure the following files are present in the project directory:

ComaperExcels.py
Excel_File_1.xlsx
Excel_File_2.xlsx


⚠️ Notes:

Files must be valid .xlsx (Excel Workbook format)

Files must be closed in Excel before running the script

5️⃣ Run Basic Comparison
python ComaperExcels.py --file1 Excel_File_1.xlsx --file2 Excel_File_2.xlsx


This will:

Compare data and structure

Generate output files in excel_compare_output/

6️⃣ Run Comparison Using a Key Column (Recommended)

If your Excel files contain a unique identifier (e.g., EmpID, ID, RecordNo):

python ComaperExcels.py --file1 Excel_File_1.xlsx --file2 Excel_File_2.xlsx --key EmpID


✅ Prevents row-shift mismatches
✅ Improves accuracy

7️⃣ Compare a Specific Sheet (Optional)
python ComaperExcels.py --file1 Excel_File_1.xlsx --file2 Excel_File_2.xlsx --sheet Data

8️⃣ Optimize Performance for Large Files

For large Excel files (50k+ rows):

python ComaperExcels.py ^
  --file1 Excel_File_1.xlsx ^
  --file2 Excel_File_2.xlsx ^
  --threads 8 ^
  --chunk 5000


(Use \ instead of ^ on Linux/macOS)

📂 Output Files

After execution, the following files are generated:

excel_compare_output/
 ├── mismatches_<timestamp>.csv
 ├── missing_in_file1.csv
 ├── missing_in_file2.csv
 └── summary.json

🔍 How to Interpret Results

summary.json

PASS → No data or structure mismatches

FAIL → Data and/or structure mismatches detected

mismatches_*.csv

STRUCTURE → Column added / removed / reordered

DATA → Actual cell value mismatches
