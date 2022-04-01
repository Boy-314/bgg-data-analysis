import csv
import matplotlib as mpl
import numpy as np
import pandas as pd
import sys
from tabulate import tabulate
from pandasql import sqldf

print("run with -v flag for verbose output\n")
verbose = False
if len(sys.argv) == 2 and sys.argv[1] == "-v":
    verbose = True

# read in the raw data from published google sheet url
print("reading in raw data...")
raw_data = pd.read_excel("https://docs.google.com/spreadsheets/d/e/2PACX-1vRMs2PYN5JnZW7mGGOE962xmVIQ5b5FTF7UkX94S795lzrGvejyqD36tHvf_MDpRcWhuxoRCHxzXY-7/pub?output=xlsx", sheet_name=["ac_raw","as_raw","ew_raw","wy_raw"])
print("success")
# variables for each person's raw data
ac_raw = raw_data["ac_raw"]
as_raw = raw_data["as_raw"]
ew_raw = raw_data["ew_raw"]
wy_raw = raw_data["wy_raw"]
# replace 0 ratings with NaN

heading = ["Game", "AC", "AS", "EW", "WY", "Average"]

###########################
### COMBINED COLLECTION ###
###########################
# sqlite has no full outer join so i just gotta do this left join union bullshit
q = """SELECT ac_raw.objectname AS game, ac_raw.rating AS ac_rating, as_raw.rating AS as_rating, ew_raw.rating AS ew_rating, wy_raw.rating AS wy_rating FROM ac_raw
    LEFT JOIN as_raw USING(objectid)
    LEFT JOIN ew_raw USING(objectid)
    LEFT JOIN wy_raw USING(objectid)
    UNION
    SELECT as_raw.objectname AS game, ac_raw.rating AS ac_rating, as_raw.rating AS as_rating, ew_raw.rating AS ew_rating, wy_raw.rating AS wy_rating FROM as_raw
    LEFT JOIN ac_raw USING(objectid)
    LEFT JOIN ew_raw USING(objectid)
    LEFT JOIN wy_raw USING(objectid)
    UNION
    SELECT ew_raw.objectname AS game, ac_raw.rating AS ac_rating, as_raw.rating AS as_rating, ew_raw.rating AS ew_rating, wy_raw.rating AS wy_rating FROM ew_raw
    LEFT JOIN ac_raw USING(objectid)
    LEFT JOIN as_raw USING(objectid)
    LEFT JOIN wy_raw USING(objectid)
    UNION
    SELECT wy_raw.objectname AS game, ac_raw.rating AS ac_rating, as_raw.rating AS as_rating, ew_raw.rating AS ew_rating, wy_raw.rating AS wy_rating FROM wy_raw
    LEFT JOIN ac_raw USING(objectid)
    LEFT JOIN as_raw USING(objectid)
    LEFT JOIN ew_raw USING(objectid)
    ORDER BY game;"""
collection = sqldf(q)
# replace 0 ratings with NaN
collection = collection.replace(0, np.NaN)
# axis = 1 -> searches column-wise and returns the mean for each row
collection["average"] = collection.mean(axis = 1, numeric_only = True)
# output number of ratings per person
print("\nratings breakdown\nAC: " + str(collection["ac_rating"].count()) + " ratings\n" + 
    "AS: " + str(collection["as_rating"].count()) + " ratings\n" + 
    "EW: " + str(collection["ew_rating"].count()) + " ratings\n" + 
    "WY: " + str(collection["wy_rating"].count()) + " ratings\n")
# verbose output
if verbose == True:
    print("combined collection")
    print(tabulate(collection, headers = "keys", tablefmt = "psql"))
collection.to_csv(path_or_buf = "bgg_combined_collection.csv", header = heading)

##################################################
### GAMES THAT ALL OF US HAVE GIVEN RATINGS TO ###
##################################################
all_rated_games = pd.DataFrame()
# remove entries with any NaN's, remaining entries are those we all rated
all_rated_games = collection.dropna()
# verbose output
if verbose == True:
    print("games all of us have rated")
    print(tabulate(all_rated_games, headers = "keys", tablefmt = "psql"))
all_rated_games.to_csv(path_or_buf = "bgg_all_rated_games.csv", header = heading)

####################################
###            STATS             ###
####################################
# Best Games all of us have rated  #
# Worst Games all of us have rated #
# Largest Rating Discrepancies     #
# Standard Deviation               #
# Correlation Coefficient          #
####################################
# Best Games all of us have rated
best_all_rated_games = pd.DataFrame()
# sort all_rated_games by average descending
sorted_all_rated_games = all_rated_games.sort_values("average", ascending = False)
best_all_rated_games["Best Agreed Upon Games"] = sorted_all_rated_games.loc[sorted_all_rated_games["average"] >= 8, "game"].head(10)
best_all_rated_games["Avg. Rating"] = sorted_all_rated_games.loc[sorted_all_rated_games["average"] >= 8, "average"].head(10)

# Worst Games all of us have rated
worst_all_rated_games = pd.DataFrame()
worst_all_rated_games["Worst Agreed Upon Games"] = sorted_all_rated_games.loc[sorted_all_rated_games["average"] < 5, "game"].tail(10)
worst_all_rated_games["Avg. Rating"] = sorted_all_rated_games.loc[sorted_all_rated_games["average"] < 5, "average"].tail(10)

# Largest Rating Discrepancies
largest_range = pd.DataFrame()
largest_range[["Most Controversial Games", "AC", "AS", "EW", "WY"]] = collection[["game", "ac_rating", "as_rating", "ew_rating", "wy_rating"]]
largest_range["Range"] = largest_range.max(axis = 1, skipna = True, numeric_only = True) - largest_range.min(axis = 1, skipna = True, numeric_only = True)
largest_range = largest_range.sort_values("Range", ascending = False).head(10)
largest_range = largest_range[["Most Controversial Games", "Range"]]

# Standard Deviation
std_dev = pd.DataFrame()
std_dev["Name"] = ["AC", "AS", "EW", "WY"]
# confusing mess to compute standard deviation while excluding 0/10 ratings (not rated but still in collection)
std_dev["Std Dev"] = [ac_raw[ac_raw["rating"] != 0]["rating"].std(), as_raw[as_raw["rating"] != 0]["rating"].std(), ew_raw[ew_raw["rating"] != 0]["rating"].std(), wy_raw[wy_raw["rating"] != 0]["rating"].std()]
std_dev = std_dev.sort_values("Std Dev", ascending = False)

# Correlation Coefficient
cor_coef = pd.DataFrame()
cor_coef["Name"] = ["AC", "AS", "EW", "WY"]
cor_coef["Correlation"] = [ac_raw[["rating", "average"]].corr(method="spearman").loc["rating"]["average"],
    as_raw[["rating", "average"]].corr(method="spearman").loc["rating"]["average"],
    ew_raw[["rating", "average"]].corr(method="spearman").loc["rating"]["average"],
    wy_raw[["rating", "average"]].corr(method="spearman").loc["rating"]["average"]]
cor_coef = cor_coef.sort_values("Correlation", key = abs, ascending = False)

# list of the statistical dataframes
stats_dfs = [best_all_rated_games, worst_all_rated_games, largest_range, std_dev, cor_coef]
# output all stats to csv
best_all_rated_games.to_csv(path_or_buf = "stats.csv", header = "keys")
with open("stats.csv", "a") as f:
    f.write("\n")
    f.close()
worst_all_rated_games.to_csv(path_or_buf = "stats.csv", header = "keys", mode = "a")
with open("stats.csv", "a") as f:
    f.write("\n")
    f.close()
largest_range.to_csv(path_or_buf = "stats.csv", header = "keys", mode = "a")
with open("stats.csv", "a") as f:
    f.write("\n")
    f.close()
std_dev.to_csv(path_or_buf = "stats.csv", header = "keys", mode = "a")
with open("stats.csv", "a") as f:
    f.write("\n")
    f.close()
cor_coef.to_csv(path_or_buf = "stats.csv", header = "keys", mode = "a")

if verbose == True:
    print("\nstats")
    for i in range(len(stats_dfs)):
        print(tabulate(stats_dfs[i], headers = "keys", tablefmt = "psql"))
