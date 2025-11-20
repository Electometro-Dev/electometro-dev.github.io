import pandas as pd
import os
import json
import re

# Define file paths – adjust as needed.
NEW_STRUCTURE_FILE = os.getenv('PERU_FILE')
OUTPUT_DIR = "json/"  # output directory

MISSING_VOTE_DEFAULT = 0.5
MISSING_COMMENT_DEFAULT = "No se encontró información pública sobre su posición."
MISSING_SOURCE_DEFAULT = None

number_of_topics = 8

def clean_text(s):
    if s is None:
        return None
    if pd.isna(s):
        return None
    s = str(s).strip()
    return s if s != "" else None

def map_vote_text_to_value(vote_text):
    if vote_text is None:
        return None

    vt = str(vote_text).strip()
    if vt == "":
        return None

    # Try numeric first, allow comma as decimal separator
    try:
        num = float(vt.replace(',', '.'))
        return num
    except Exception:
        pass
    return None

def parse_cell_combined(cell_value):
    """
    Parse a single cell of the form:
    "A favor***Comment X***Source X" or "0***Comment***Source"
    into (vote_value_mapped, comment, source).
    If the cell is empty or missing, default to 0.5, "No se encontró...", None.
    """
    raw = clean_text(cell_value)
    if raw is None:
        return MISSING_VOTE_DEFAULT, MISSING_COMMENT_DEFAULT, MISSING_SOURCE_DEFAULT

    parts = raw.split('***', 2)  # max 3 parts
    vote_part   = clean_text(parts[0]) if len(parts) >= 1 else None
    comment_part= clean_text(parts[1]) if len(parts) >= 2 else None
    source_part = clean_text(parts[2]) if len(parts) >= 3 else None

    vote_mapped = map_vote_text_to_value(vote_part)

    return vote_mapped, comment_part, source_part

def generate_from_new_structure():
    """
    Read the Excel file in the "presidencial" sheet, build a JSON structure of candidates and their votes.
    If a cell for a candidate is missing, write vote=0.5, comment="No se encontró...", source=None.
    Do not drop empty party columns, do not skip questions when all cells are missing.
    """
    # Defaults for truly missing cells
    missing_vote_default = 0.5
    missing_comment_default = "No se encontró..."
    missing_source_default = None

    # Read the raw sheet, first row contains headers
    raw_dataframe = pd.read_excel(
        NEW_STRUCTURE_FILE,
        sheet_name="parlamentaria",
        dtype=str,
        header=None
    )

    raw_dataframe = raw_dataframe.head(number_of_topics + 1)
    if raw_dataframe.shape[0] < 1:
        raise ValueError("Input sheet appears empty or is missing the header row.")

    # Promote first row to headers, drop that row
    raw_dataframe.columns = raw_dataframe.iloc[0]
    data_frame = raw_dataframe.drop(index=0).reset_index(drop=True)

    # Validate required columns
    if "Statement" not in data_frame.columns or "Tema" not in data_frame.columns:
        raise ValueError("Expected 'Tema' and 'Statement' columns in the sheet.")

    # party columns are every column except the two metadata columns
    party_columns = [column_name for column_name in data_frame.columns if column_name not in ("Tema", "Statement")]

    # Prepare party metadata and output structure
    party_info = {}
    for party_column in party_columns:
        party_name = party_column
        party_info[party_column] = {
            "header": party_column,
            "name": party_name,
            "votes": {}
        }

    number_of_rows = data_frame.shape[0]

    # Iterate each row as one question
    for row_index in range(number_of_rows):
        topic_raw_value = data_frame.at[row_index, "Tema"] if "Tema" in data_frame.columns else None
        statement_raw_value = data_frame.at[row_index, "Statement"] if "Statement" in data_frame.columns else None

        topic_text = clean_text(topic_raw_value)
        statement_text = clean_text(statement_raw_value)

        # Skip rows without a statement
        if statement_text is None:
            continue

        # Build a readable question identifier
        question_identifier = f"{topic_text}: {statement_text}" if topic_text else statement_text

        # For every party, parse the cell, default if missing
        for party_column in party_columns:
            cell_value = data_frame.at[row_index, party_column] if party_column in data_frame.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            # If the cell is truly missing or empty, enforce defaults
            if vote_value is None and comment_value is None and source_value is None:
                vote_value = missing_vote_default
                comment_value = missing_comment_default
                source_value = missing_source_default

            party_info[party_column]["votes"][question_identifier] = {
                "tema": topic_text,
                "question": statement_text,
                "vote": vote_value,
                "comment": comment_value,
                "source": source_value
            }

    # Build the final combined structure
    combined_output = {"parties": {}}
    for party_column, party_info in party_info.items():
        combined_output["parties"][party_info["header"]] = {
            "name": party_info["name"],
            "votes": party_info["votes"]
        }

    # Ensure output directory exists, then write JSON file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "combined_votes_peru_partidos_2026.json")
    with open(output_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    generate_from_new_structure()
