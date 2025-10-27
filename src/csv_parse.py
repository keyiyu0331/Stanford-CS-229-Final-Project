import pandas as pd
import os
import re  # Added for splitting ingredients
import sys

def process_ingredients(s):
    """
    Converts an ingredient string into a cleaned list of ingredients.
    - Lowercases
    - Splits on commas, ' and ', or parentheses
    - Returns an empty list if no data
    """
    if not isinstance(s, str) or not s:
        return []
    
    # Lowercase and split on commas, ' and ', or parentheses
    s_low = s.lower()
    items = re.split(r'\s*,\s*|\s+and\s+|\s*[\(\)]\s*', s_low)
    
    # Return list, stripping whitespace and removing empty strings
    return [item.strip() for item in items if item.strip()]

def create_nutrition_csvs_final(folder_path, output_path):
    """
    Loads USDA food database CSVs from a specified folder and processes them
    into two summary CSVs.
    
    This version:
    - Includes ALL foods of valid types.
    - Removes nutrient columns with > 50% missing data.
    - Adds the food's category.
    - If 'branded_food.csv' is found, it adds brand/owner to the
      description and adds an 'ingredients' column as a list.
    
    Args:
        folder_path: Path to folder containing input CSV files
        output_path: Path to output directory
    """
    
    # Create output directory if it doesn't exist
    os.makedirs(output_path, exist_ok=True)
    
    # --- Configuration ---
    
    # Define which food types to include in the final output.
    VALID_FOOD_TYPES = ['foundation_food', 'sr_legacy_food', 'survey_fndds_food', 'branded_food']

    # Map nutrient units to their conversion factor to grams.
    UNIT_TO_GRAM_FACTOR = {
        'G': 1.0,
        'MG': 0.001,  # Milligrams to Grams
        'UG': 1e-6,   # Micrograms to Grams
    }
    
    # --- 1. Load Data ---
    
    print("Loading data files...")
    
    try:
        # All file paths now use os.path.join to read from the provided folder_path
        df_food = pd.read_csv(os.path.join(folder_path, "food.csv"), low_memory=False)
        df_nutrient = pd.read_csv(os.path.join(folder_path, "nutrient.csv"), low_memory=False)
        df_food_nutrient = pd.read_csv(os.path.join(folder_path, "food_nutrient.csv"), low_memory=False)
        df_portion = pd.read_csv(os.path.join(folder_path, "food_portion.csv"), low_memory=False)
        df_measure_unit = pd.read_csv(os.path.join(folder_path, "measure_unit.csv"), low_memory=False)
        df_category = pd.read_csv(os.path.join(folder_path, "food_category.csv"), low_memory=False)
        
        # --- Attempt to load branded_food.csv ---
        try:
            df_branded = pd.read_csv(os.path.join(folder_path, "branded_food.csv"), low_memory=False)
            print("Successfully loaded branded_food.csv.")
        except FileNotFoundError:
            df_branded = None
            print("Warning: 'branded_food.csv' not found. Brand and ingredient info will be blank.")
        
    except FileNotFoundError as e:
        print(f"Error: Missing required file: {e.filename}")
        print(f"Please make sure all CSV files (food.csv, nutrient.csv, etc.) are in the directory: {folder_path}")
        return

    print("Data loading complete.")

    # --- 2. Prepare Nutrient Mappings ---

    print("Preparing nutrient mappings and unit conversions...")

    df_nutrient['conversion_factor'] = df_nutrient['unit_name'].map(UNIT_TO_GRAM_FACTOR).fillna(1.0)
    df_nutrient['final_unit'] = df_nutrient['unit_name'].apply(lambda x: 'G' if x in UNIT_TO_GRAM_FACTOR else x)
    df_nutrient['column_name'] = df_nutrient['name'] + ' (' + df_nutrient['final_unit'] + ')'

    id_to_column = df_nutrient.set_index('id')['column_name'].to_dict()
    id_to_converter = df_nutrient.set_index('id')['conversion_factor'].to_dict()

    all_nutrient_columns = sorted(list(df_nutrient['column_name'].unique()))

    # --- 3. Filter and Process Food Data ---

    print("Filtering and standardizing food data...")

    # Filter main food list to valid types
    # --- FIX: Add .copy() to prevent SettingWithCopyWarning ---
    df_food_filtered = df_food[df_food['data_type'].isin(VALID_FOOD_TYPES)].copy()
    
    # Add Category Information
    print("Adding food category descriptions...")
    df_category = df_category.rename(columns={'description': 'category'})
    
    # --- ERROR FIX HERE ---
    df_food_filtered['food_category_id'] = pd.to_numeric(
        df_food_filtered['food_category_id'], errors='coerce'
    )
    # --- END FIX ---

    df_food_filtered = df_food_filtered.merge(
        df_category[['id', 'category']],
        left_on='food_category_id',
        right_on='id',
        how='left'
    )
    df_food_filtered['category'] = df_food_filtered['category'].fillna('Unknown')
    
    # --- Add Brand and Ingredient Information ---
    if df_branded is not None:
        print("Adding brand and ingredient data...")
        # Select relevant columns
        # --- FIX: Add .copy() to prevent SettingWithCopyWarning ---
        df_brand_info = df_branded[['fdc_id', 'brand_owner', 'brand_name', 'ingredients']].copy()
        
        # --- NEW: Process ingredients into a list ---
        df_brand_info['ingredients'] = df_brand_info['ingredients'].apply(process_ingredients)
        # --- END NEW ---
        
        # Clean up brand text data
        df_brand_info['brand_owner'] = df_brand_info['brand_owner'].fillna('')
        df_brand_info['brand_name'] = df_brand_info['brand_name'].fillna('')

        # Create brand prefix
        def create_brand_prefix(row):
            if row['brand_name'] and row['brand_name'] != row['brand_owner']:
                return f"{row['brand_owner']} - {row['brand_name']}: "
            elif row['brand_owner']:
                return f"{row['brand_owner']}: "
            else:
                return ""
        
        df_brand_info['brand_prefix'] = df_brand_info.apply(create_brand_prefix, axis=1)
        
        # Merge brand info into the main food dataframe
        df_food_filtered = df_food_filtered.merge(
            df_brand_info[['fdc_id', 'brand_prefix', 'ingredients']],
            on='fdc_id',
            how='left'
        )
        
        # Fill missing brand/ingredients for non-branded foods
        df_food_filtered['brand_prefix'] = df_food_filtered['brand_prefix'].fillna('')
        
        # --- NEW: Fill missing ingredients with an empty list ---
        df_food_filtered['ingredients'] = df_food_filtered['ingredients'].apply(
            lambda x: x if isinstance(x, list) else []
        )
        # --- END NEW ---
        
        # Prepend brand to description
        df_food_filtered['description'] = df_food_filtered['brand_prefix'] + df_food_filtered['description']
        
    else:
        # If branded_food.csv wasn't loaded, create empty ingredients column
        # --- NEW: Ensure it's an empty list for type consistency ---
        df_food_filtered['ingredients'] = [[] for _ in range(len(df_food_filtered))]
        # --- END NEW ---
    
    all_valid_fdc_ids = set(df_food_filtered['fdc_id'])
    
    if not all_valid_fdc_ids:
        print(f"Error: No foods found for the valid types: {VALID_FOOD_TYPES}")
        return
        
    print(f"Found {len(all_valid_fdc_ids)} foods of valid types.")

    df_fn_filtered = df_food_nutrient[df_food_nutrient['fdc_id'].isin(all_valid_fdc_ids)].copy()
    converters = df_fn_filtered['nutrient_id'].map(id_to_converter)
    df_fn_filtered['amount_std'] = df_fn_filtered['amount'] * converters

    # --- 4. Prepare Data for Pivoting ---

    print(f"Preparing data for all {len(all_valid_fdc_ids)} valid foods.")

    df_fn_filtered['column_name'] = df_fn_filtered['nutrient_id'].map(id_to_column)
    df_fn_pivot_ready = df_fn_filtered[['fdc_id', 'column_name', 'amount_std']]
    df_fn_pivot_ready = df_fn_pivot_ready.drop_duplicates(subset=['fdc_id', 'column_name'])

    # --- 5. Create Wide Data & Filter Sparse Columns ---

    print("Pivoting data and filling missing values...")

    df_wide = df_fn_pivot_ready.pivot(
        index='fdc_id',
        columns='column_name',
        values='amount_std'
    )

    df_wide_all_cols = df_wide.reindex(columns=all_nutrient_columns, fill_value=-1.0)
    df_wide_all_cols = df_wide_all_cols.fillna(-1.0)
    
    print("Checking for sparsely populated nutrient columns...")
    total_foods = len(df_wide_all_cols)
    threshold = total_foods / 2.0
    columns_to_drop = []
    
    for col in all_nutrient_columns:
        missing_count = (df_wide_all_cols[col] == -1.0).sum()
        if missing_count > threshold:
            columns_to_drop.append(col)
            
    if columns_to_drop:
        print(f"Dropping {len(columns_to_drop)} columns with > 50% missing data.")
        df_wide_filtered = df_wide_all_cols.drop(columns=columns_to_drop)
        final_nutrient_columns = [col for col in all_nutrient_columns if col not in columns_to_drop]
    else:
        print("No sparse columns found. Keeping all nutrients.")
        df_wide_filtered = df_wide_all_cols
        final_nutrient_columns = all_nutrient_columns

    print(f"Kept {len(final_nutrient_columns)} nutrient columns.")
    
    # --- 6. Create 'foods_per_100g.csv' ---

    print("Creating 'foods_per_100g.csv'...")
    
    # Join with the food descriptions - ADDED 'category' and 'ingredients'
    df_100g = df_food_filtered[['fdc_id', 'description', 'category', 'ingredients']].merge(
        df_wide_filtered,  # Use the filtered DataFrame
        on='fdc_id'
    )
    
    df_100g['fdc_id'] = df_100g['fdc_id'].astype(int)
    # Re-order columns to put category and ingredients up front
    cols_100g = ['fdc_id', 'description', 'category', 'ingredients'] + final_nutrient_columns
    df_100g = df_100g[cols_100g]
    df_100g = df_100g.sort_values(by='description').reset_index(drop=True)

    # Save the per-100g CSV
    output_100g_path = os.path.join(output_path, "foods_per_100g.csv")
    df_100g.to_csv(output_100g_path, index=False, float_format='%.4f')
    print(f"Successfully created '{output_100g_path}' with {len(df_100g)} foods.")

    # --- 7. Create 'foods_per_serving.csv' ---

    print("Processing serving data...")

    df_portion_filtered = df_portion[
        df_portion['fdc_id'].isin(all_valid_fdc_ids) &
        df_portion['gram_weight'].notna() &
        (df_portion['gram_weight'] > 0)
    ]
    
    if df_portion_filtered.empty:
        print("Warning: No valid portion data found for the filtered foods. Skipping 'foods_per_serving.csv'.")
        print("\nAll done!")
        return

    df_portion_full = df_portion_filtered.merge(df_measure_unit, left_on='measure_unit_id', right_on='id', suffixes=('_portion', '_unit'))

    df_portion_full['serving_description'] = df_portion_full.apply(
        lambda row: row['portion_description'] if pd.notna(row['portion_description']) and row['portion_description'].strip()
        else f"{row['amount']} {row['name']}",
        axis=1
    )
    
    df_servings_base = df_portion_full[['fdc_id', 'serving_description', 'gram_weight']]
    df_servings_base = df_servings_base.rename(columns={'gram_weight': 'serving_size_g'})
    df_servings_base = df_servings_base.drop_duplicates()

    print(f"Found {len(df_servings_base)} unique servings.")

    # Join serving data with the *filtered* per-100g nutrient data
    df_per_serving = df_servings_base.merge(
        df_wide_filtered,  # Use the filtered DataFrame
        on='fdc_id'
    )
    
    print("Calculating per-serving nutrient values...")

    # Apply scaling factor to all *remaining* nutrient columns
    for col in final_nutrient_columns: # Use the filtered list
        if col in df_per_serving.columns:
            # Only scale if the value is not -1 (missing)
            df_per_serving[col] = df_per_serving.apply(
                lambda row: row[col] * (row['serving_size_g'] / 100.0) if row[col] != -1.0 else -1.0,
                axis=1
            )
            
    # Join with food descriptions - ADDED 'category' and 'ingredients'
    df_per_serving_final = df_food_filtered[['fdc_id', 'description', 'category', 'ingredients']].merge(
        df_per_serving,
        on='fdc_id'
    )
    
    # Re-order columns - ADDED 'category' and 'ingredients'
    final_serving_cols = ['fdc_id', 'description', 'category', 'ingredients', 'serving_description', 'serving_size_g'] + final_nutrient_columns # Use filtered list
    df_per_serving_final = df_per_serving_final[final_serving_cols]
    
    df_per_serving_final = df_per_serving_final.sort_values(by=['description', 'serving_description']).reset_index(drop=True)
    
    # Save the per-serving CSV
    output_serving_path = os.path.join(output_path, "foods_per_serving.csv")
    df_per_serving_final.to_csv(output_serving_path, index=False, float_format='%.4f')
    
    print(f"Successfully created '{output_serving_path}' with {len(df_per_serving_final)} food/serving combinations.")
    print("\nAll done!")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python csv_parse.py <input_folder> <output_folder>")
        print("  input_folder: Path to folder containing USDA CSV files")
        print("  output_folder: Path to output directory")
        sys.exit(1)
        
    csv_folder_path = sys.argv[1]
    output_folder_path = sys.argv[2]
    
    if not os.path.isdir(csv_folder_path):
        print(f"Error: Path not found or is not a directory: {csv_folder_path}")
        sys.exit(1)
        
    create_nutrition_csvs_final(csv_folder_path, output_folder_path)