This README document contains instructions for the 'Less Account Scenario Building' 
part of Renter Profitability.
---------------------------------------------------------------------------------------------------
The purpose of this codebase is to do the following tasks:
	1. Create the appropriate user-defined columns in a Cosmic Frog optimization model,
	   and populate them with data. (Python)
	2. Create the appropriate scenarios in a Cosmic Frog optimization model. (Alteryx)

Task 1 above is done through the following two python scripts:
	01-create-transfer-matrix-excel.py
	02-create-user-defined-columns.py

Task 2 above is done through the following three Alteryx workflows:
	1_CF_Inputs.yxmc
	3_Return Profile Creation - Output.yxmc
	4_Account Model Creatin.yxmc

---------------------------------------------------------------------------------------------------
The folder structor of this project should be as follows:
(Note: a "/" character after the name means it is a folder).

less-account-scenario-building/
	000-Transfer Matrix (All)/
	  .
	  .
	  .
	001-Transfer Matrix (Most Recent 12 Months)/
	  .
	  .
	  .
	010-Renter Profitability Output/
	  .
	  .
	  .
	01-create-transfer-matrix-excel.py
	02-create-user-defined-columns.py
	1_CF_Inputs.yxmc
	3_Return Profile Creation - Output.yxmc
	4_Account Model Creation.yxmc
	Account List.xlsx
	README.txt
	TransferMatric_RenterToReturnLocation.xlsx
---------------------------------------------------------------------------------------------------
To run this code, do the following:

	1. Ensure that the 12 most recent Transfer Matrix excel files are in the 
	   001-Transfer Matrix (Most Recent 12 Months) folder.
	2. Ensure that the Account List.xlsx file has all accounts you wish to analyse labelled 
	   with a "Y" in the "Evaluate" column (column C).
	3. Update the user inputs in 01-create-transfer-matrix-excel.py and RUN the file.
	4. Update the user inputs in 02-create-user-defined-columns.py and RUN the file.
	5. Update the user inputs in 1_CF_Inputs.yxmc and SAVE the file.
	6. Update the user inputs in 3_Return Profile Creation - Output.yxmc and SAVE the file.
	7. Update the user inputs in 4_Account Model Creation.yxmc and RUN the file.
