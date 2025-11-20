import pandas as pd
import os
import json
import re

# Define file paths – adjust as needed.
NEW_STRUCTURE_FILE = os.getenv('CHILE_FILE')
OUTPUT_DIR = "json/"  # output directory

MISSING_VOTE_DEFAULT = 0.5
MISSING_COMMENT_DEFAULT = "No se encontró información pública sobre su posición."
MISSING_SOURCE_DEFAULT = None

def clean_text(s):
    if s is None:
        return None
    if pd.isna(s):
        return None
    s = str(s).strip()
    return s if s != "" else None

def parse_candidate_header(header_str):
    if header_str is None:
        return None, None
    header_str = str(header_str).strip()
    m = re.match(r'^(.*?)\s*\((.*?)\)\s*$', header_str)
    if m:
        name = m.group(1).strip()
        party = m.group(2).strip()
    else:
        name = header_str
        party = None
    return name, party

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

    return vote_part, comment_part, source_part

def generate_from_new_structure():
    # Read the raw sheet, first row contains headers
    raw_dataframe = pd.read_excel(
        NEW_STRUCTURE_FILE,
        sheet_name="presidencial",
        dtype=str,
        header=None
    )

    raw_dataframe = raw_dataframe.head(21)
    if raw_dataframe.shape[0] < 1:
        raise ValueError("Input sheet appears empty or is missing the header row.")

    # Promote first row to headers, drop that row
    raw_dataframe.columns = raw_dataframe.iloc[0]
    data_frame = raw_dataframe.drop(index=0).reset_index(drop=True)

    # Validate required columns
    if "Statement" not in data_frame.columns or "Tema" not in data_frame.columns:
        raise ValueError("Expected 'Tema' and 'Statement' columns in the sheet.")

    # Candidate columns are every column except the two metadata columns
    candidate_columns = [column_name for column_name in data_frame.columns if column_name not in ("Tema", "Statement")]

    # Prepare candidate metadata and output structure
    candidates_info = {}
    for candidate_column in candidate_columns:
        candidate_name, candidate_party = parse_candidate_header(candidate_column)
        candidates_info[candidate_column] = {
            "header": candidate_column,
            "name": candidate_name,
            "party": candidate_party,
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

        # For every candidate, parse the cell, default if missing
        for candidate_column in candidate_columns:
            cell_value = data_frame.at[row_index, candidate_column] if candidate_column in data_frame.columns else None
            vote_value, comment_value, source_value = parse_cell_combined(cell_value)

            # If the cell is truly missing or empty, enforce defaults
            if vote_value is None and comment_value is None and source_value is None:
                vote_value = MISSING_VOTE_DEFAULT
                comment_value = MISSING_COMMENT_DEFAULT
                source_value = MISSING_SOURCE_DEFAULT

            candidates_info[candidate_column]["votes"][question_identifier] = {
                "tema": topic_text,
                "question": statement_text,
                "vote": vote_value,
                "comment": comment_value,
                "source": source_value
            }

    # Build the final combined structure
    combined_output = {"candidates": {}}
    for candidate_column, candidate_info in candidates_info.items():
        combined_output["candidates"][candidate_info["header"]] = {
            "name": candidate_info["name"],
            "party": candidate_info["party"],
            "votes": candidate_info["votes"]
        }

    # Ensure output directory exists, then write JSON file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, "combined_votes_chile_pres_2025.json")
    with open(output_path, "w", encoding="utf-8") as file_handle:
        json.dump(combined_output, file_handle, ensure_ascii=False, indent=2)

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    generate_from_new_structure()
