import pandas as pd
import json
import random

def generate_businesses(
  num_biz = 1000, param_file = "entity_params.json",
  criminal_biz_proportion = 0.01
  ):

  print("Started Reading JSON file")
  with open(param_file, "r") as read_file:
      print("Converting JSON encoded data into Python dictionary")
      entity_params_dict = json.load(read_file)

  # Create num_biz business ids
  entity_id = ["B_" + str(x) for x in range(1, num_biz + 1)]

  # Create business region by sampling from region distribution in param dict
  region_list = list(entity_params_dict["business"]["region_dist"].keys())
  region_weights = list(entity_params_dict["business"]["region_dist"].values())

  region = random.choices(region_list, weights=region_weights, k=num_biz)

  # Create business sector by sampling from conditional distribution of sector
  # given region from param dict

  sector = []
  for rgn in region:
      sector_list = list(entity_params_dict["business"]["region_sector_dist"][rgn].keys())
      sector_weights = list(entity_params_dict["business"]["region_sector_dist"][rgn].values())
      sector.append(random.choices(sector_list, weights = sector_weights, k=1)[0])

  # Create business sector by sampling from conditional distribution of turnover
  # given region from param dict

  turnover = []
  for rgn in region:
      turnover_list = list(entity_params_dict["business"]["region_turnover_dist"][rgn].keys())
      turnover_weights = list(entity_params_dict["business"]["region_turnover_dist"][rgn].values())
      turnover.append(random.choices(turnover_list, weights = turnover_weights, k=1)[0])

  # Create account ids, for now each business has a current and a loan account
  # account ids are just the entity_id with the suffic CUR or LOA

  account_ids = []
  for entity in entity_id:
      account_ids.append([str(entity) + '_CUR', str(entity) + '_LOA'])

  # Create criminal flag
  criminal = random.choices([0,1], weights = [1-criminal_biz_proportion, criminal_biz_proportion], k=num_biz)

  pd.DataFrame({
      "entity_id" : entity_id,
      "region" : region,
      "sector" : sector,
      "turnover" : turnover,
      "account_ids" : account_ids,
      "criminal" : criminal
  }).to_csv("businesses.csv")

generate_businesses()





