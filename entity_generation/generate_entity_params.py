import pandas as pd
import re
import json

# Load the ONS data
sector_freq_df = pd.read_excel("../data/ukbusinessworkbook2020.xlsx", sheet_name = "Table 1")

# Create series of region names and region frequencies, offset by 1 forward
# so that when you select the rows that are NaNs
# you in fact select the row AFTER the NaN
region_names = sector_freq_df.iloc[6:450,1]
region_frequency = sector_freq_df.iloc[6:450,19]

# Create boolean list indicating location of NaNs
names_index = list(sector_freq_df.iloc[5:449,1].isnull())

# Pick our names and frequencies for high level regions - which occur AFTER
# the NaN values in the data
region_names_list = list(region_names[names_index])
region_names_list = [x.rstrip() for x in region_names_list]
region_freqency_list = list(region_frequency[names_index])

# Create series of lad17 codes and select the ones that map to the
# region_names_list
lad17cd_codes = sector_freq_df.iloc[6:450,0]
lad17cd_codes_list = list(lad17cd_codes[names_index])

# Create region distribution dictionary and map of region name to lad code
region_dist = dict(zip(region_names_list, region_freqency_list))
region_lad_map = dict(zip(region_names_list, lad17cd_codes_list))


################################################################################
#################### Create joint dist of (region, sector) #####################
################################################################################

# Create list of sector names
sector_name = list(sector_freq_df.iloc[4,2:19])
# Remove unwanted characters from names
sector_name = [" ".join(re.findall("[a-zA-Z]+", x)) for x in sector_name]

# Create df of region-sector frequencies, offset by 1 forward
# so that when you select the rows that are NaNs
# you in fact select the row AFTER the NaN
region_sector_frequency = sector_freq_df.iloc[6:450,2:19]
# select only the high level regions
region_sector_frequency_high_level = pd.DataFrame(region_sector_frequency[names_index])
# Rename index and columns
region_sector_frequency_high_level.index = region_names_list
region_sector_frequency_high_level.columns = sector_name

# Convert region_sector_frequency_high_level df to a dictionary
# with the following structure:
# {"region": {"sector_1": count, "sector_2": count,...}}
region_sector_string = region_sector_frequency_high_level.to_json(orient="index")
region_sector_dist = json.loads(region_sector_string)

################################################################################
################### Create joint dist of (region, turnover) ####################
################################################################################

# Load the ONS data
turnover_freq_df = pd.read_excel("../data/ukbusinessworkbook2020.xlsx", sheet_name = "Table 7")

# Create list of turnover bands
turnover_bands = list(turnover_freq_df.iloc[4,1:11])
# Replace NaNs with ""
turnover_freq_df["Unnamed: 0"] = turnover_freq_df["Unnamed: 0"].fillna("")

# Remove unwanted characters
turnover_freq_df["Unnamed: 0"] = [" ".join(re.findall("[a-zA-Z]+", x))[2:] for x in turnover_freq_df.iloc[:,0]]

# Create df of region-turnover frequencies
region_turnover_frequency = turnover_freq_df.iloc[5:683,1:11]
# Select only those rows for the high level regions
region_turnover_frequency_high_level = region_turnover_frequency[list(turnover_freq_df.iloc[5:683,0].isin(region_names_list))]
# Rename index and columns
region_turnover_frequency_high_level.index = region_names_list
region_turnover_frequency_high_level.columns = turnover_bands

# Convert region_turnover_frequency_high_level df to a dictionary
# with the following structure:
# {"region": {"turnover_1": count, "turnover_2": count,...}}
region_turnover_string = region_turnover_frequency_high_level.to_json(orient="index")
region_turnover_dist = json.loads(region_turnover_string)

################################################################################
##################### Create entity dictionary and save it #####################
################################################################################

# Create dictionary with all entity parameters
entity_dict = {"business":{
    "region_dist" : region_dist,
    "region_lad_map": region_lad_map,
    "region_sector_dist" : region_sector_dist,
    "region_turnover_dist": region_turnover_dist
}}

# Write entity parameter dict to json
with open("entity_params.json", "w") as outfile:
    json.dump(entity_dict, outfile, indent = 4)
