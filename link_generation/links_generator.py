import pandas as pd
import random

num_links = 1000 # number of links to generate
num_ind_accts = 100 # number of individual accounts
num_biz_accts = 10 # number of businuess accounts

txn_types = ["retail_purchase","supplier","salary"]
txn_proportions_dict = [80, 15, 5] # !!!weight correspond to order of txn_types above!!!

def create_accounts(num_ind_accts, num_biz_accts):
    tot_accts = num_ind_accts + num_biz_accts
    acct_ids = []
    for acct_idx in range(tot_accts):
        if acct_idx < num_ind_accts:
            acct_ids.append("I_" + str(acct_idx) + "_curr")
        else:
            acct_ids.append("B_" + str(acct_idx) + "_curr")
    return acct_ids

acct_ids = create_accounts(num_ind_accts, num_biz_accts)
ind_acct_ids = [x for x in acct_ids if x[0]=='I']
biz_acct_ids = [x for x in acct_ids if x[0]=='B']

link_types_list = random.choices(txn_types, weights=txn_proportions_dict, k=num_links)

def create_retail_link(ind_acct_ids, biz_acct_ids):
    link_features_dict = {}
    link_features_dict['type'] = 'retail_purchase'
    link_features_dict['account_from'] = random.choices(ind_acct_ids)[0]
    link_features_dict['account_to'] = random.choices(biz_acct_ids)[0]
    link_features_dict['amount_range'] = int(random.uniform(30,300))
    link_features_dict['txn_prob'] = 0.8
    link_features_dict['MoP'] = 'FPS' # Faster payments
    return link_features_dict

def create_supplier_link(ind_acct_ids, biz_acct_ids):
    link_features_dict = {}
    link_features_dict['type'] = 'supplier'
    link_features_dict['account_from'] = random.choices(biz_acct_ids)[0]
    link_features_dict['account_to'] = random.choices(biz_acct_ids)[0]
    link_features_dict['amount_range'] = int(random.uniform(1000,10000))
    link_features_dict['txn_prob'] = 0.8
    link_features_dict['MoP'] = 'BACS'
    return link_features_dict

def create_salary_link(ind_acct_ids, biz_acct_ids):
    link_features_dict = {}
    link_features_dict['type'] = 'salary'
    link_features_dict['account_from'] = random.choices(biz_acct_ids)[0]
    link_features_dict['account_to'] = random.choices(ind_acct_ids)[0]
    link_features_dict['amount_range'] = int(random.uniform(1000,9000))
    link_features_dict['txn_prob'] = 0.033
    link_features_dict['MoP'] = 'BACS'
    return link_features_dict

link_creator_map = {"retail_purchase" : create_retail_link,
                    "supplier" : create_supplier_link,
                    "salary" : create_salary_link}

def create_links(link_types_list, ind_acct_ids, biz_acct_ids):
    link_features_list = []
    for link_type in link_types_list:
        # Get the link_creator_func function from link_creator_map dictionary
        link_creator_func = link_creator_map.get(link_type)
        # Execute the link_creator_func function
        link_features_list.append(link_creator_func(ind_acct_ids, biz_acct_ids))

    return pd.DataFrame(link_features_list)

links_df = create_links(link_types_list, ind_acct_ids, biz_acct_ids)
links_df.to_csv("links_df.csv", index = False)
