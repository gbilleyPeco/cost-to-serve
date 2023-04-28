import pandas as pd
import os

TM_FOLDER = '001-Transfer Matrix (Most Recent 12 Months)'
OUTPUT_FILENAME = 'TransferMatrix_RenterToReturnLocation.xlsx'

print(f'Creating {OUTPUT_FILENAME}...')

files = os.listdir(TM_FOLDER)
assert len(files) == 12, f'There are not exactly 12 files in the folder: {TM_FOLDER}.'

# Read individual monthly files and union together into one dataframe.
transfer_matrix = pd.DataFrame()
for f in files:
    new_df = pd.read_excel(os.path.join(TM_FOLDER, f))
    transfer_matrix = pd.concat([transfer_matrix, new_df])

# Aggregate unioned dataframe.
groupby_cols = ['Renter Corp Code', 'Renter Corp Desc', 'Return Loc Code', 'Return Loc Desc',
                'Return Corp Code', 'Return Corp Desc']
transfer_matrix_agg = transfer_matrix.groupby(groupby_cols)['Return Volume'].sum().reset_index()

# Save file.
transfer_matrix_agg.to_excel(OUTPUT_FILENAME)

print('\tDone.')