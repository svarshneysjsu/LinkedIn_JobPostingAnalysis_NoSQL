import pandas as pd
import numpy as np
import csv
import re


# import all the CSVs
job_postings = pd.read_csv("raw_data/job_postings.csv")
companies = pd.read_csv("raw_data/company_details/companies.csv")
company_industries = pd.read_csv("raw_data/company_details/company_industries.csv")
company_specialities = pd.read_csv("raw_data/company_details/company_specialities.csv")
employee_counts = pd.read_csv("raw_data/company_details/employee_counts.csv")
benefits = pd.read_csv("raw_data/job_details/benefits.csv")
job_industries = pd.read_csv("raw_data/job_details/job_industries.csv")
job_skills = pd.read_csv("raw_data/job_details/job_skills.csv")


# cleanup companies
idx = companies.duplicated(subset=list(companies.columns.difference(["company_id"])))
print(f"Number of rows in companies before removing duplicates - {len(companies)}")
print(
    f"Number of duplicate rows in companies, where only the company_id is different, but all other columns are equal - {idx.sum()}"
)
companies.drop(companies[idx].index, inplace=True)
print(f"Number of rows in companies before removing duplicates - {len(companies)}")
print()


# cleanup company specialities
company_specialities["speciality"] = company_specialities["speciality"].str.lower()
print(
    f"Number of rows in company_specialities before removing duplicate entries - {len(company_specialities)}"
)
company_specialities.drop_duplicates(inplace=True)
print(
    f"Number of rows in company_specialities after removing duplicate entries - {len(company_specialities)}"
)
print(f"Number of rows before exploding multivalued rows - {len(company_specialities)}")


def split_specialities(specialities):
    parts = re.split(r",(?![^\(]*\))", specialities)
    return [part.strip() for part in parts]


company_specialities["speciality"] = company_specialities["speciality"].apply(
    split_specialities
)
company_specialities = company_specialities.explode("speciality")
company_specialities.reset_index(drop=True, inplace=True)
print(f"Number of rows after exploding multivalued rows - {len(company_specialities)}")
print(
    f"Number of rows in company_specialities before removing duplicate entries - {len(company_specialities)}"
)
company_specialities.drop_duplicates(inplace=True)
print(
    f"Number of rows in company_specialities after removing duplicate entries - {len(company_specialities)}"
)
a = set(companies["company_id"])
b = set(company_specialities["company_id"])
print(
    f"Number of rows in company_specialities before enforcing foreign key constraint - {len(company_specialities)}"
)
company_specialities.drop(
    company_specialities[company_specialities["company_id"].isin(b - a)].index,
    inplace=True,
)
print(
    f"Number of rows in company_specialities after enforcing foreign key constraint - {len(company_specialities)}"
)
print(
    f"Number of rows in companies whose company don't have any company_specialities associated with it - {len(a - b)}"
)
print()


# cleanup company industries
print(
    f"Number of rows in company_industries before removing duplicate entries - {len(company_industries)}"
)
company_industries.drop_duplicates(inplace=True)
print(
    f"Number of rows in company_industries after removing duplicate entries - {len(company_industries)}"
)
a = set(companies["company_id"])
b = set(company_industries["company_id"])
print(
    f"Number of rows in company_industries before enforcing foreign key constraint - {len(company_industries)}"
)
company_industries.drop(
    company_industries[company_industries["company_id"].isin(b - a)].index, inplace=True
)
print(
    f"Number of rows in company_industries after enforcing foreign key constraint - {len(company_industries)}"
)
print(
    f"Number of rows in companies whose company don't have any company_industries associated with it - {len(a - b)}"
)
print()


# cleanup employee counts
print(
    f"Number of rows in employee_counts before removing duplicate entries - {len(employee_counts)}"
)
employee_counts.drop_duplicates(inplace=True)
print(
    f"Number of rows in employee_counts after removing duplicate entries - {len(employee_counts)}"
)
a = set(companies["company_id"])
b = set(employee_counts["company_id"])
print(
    f"Number of rows in employee_counts before enforcing foreign key constraint - {len(employee_counts)}"
)
employee_counts.drop(
    employee_counts[employee_counts["company_id"].isin(b - a)].index, inplace=True
)
print(
    f"Number of rows in employee_counts after enforcing foreign key constraint - {len(employee_counts)}"
)
print(
    f"Number of rows in companies whose company don't have any employee_counts associated with it - {len(a - b)}"
)
print()


# cleaning job postings
job_postings["company_id"] = job_postings["company_id"].fillna(0).astype(np.int64)
a = set(job_postings["company_id"])
b = set(companies["company_id"])
print(
    f"Number of unique company_id in companies which doesn't exist in job_postings - {len(b - a)}"
)
print(
    f"Number of unique company_id in job_postings which doesn't exist in companies - {len(a - b)}"
)
print(
    f"Replacing company_id in job_postings where company_id doesn't exist to 0 (Null) ..."
)
job_postings.loc[job_postings["company_id"].isin((a - {0}) - b), "company_id"] = 0
a = set(job_postings["company_id"])
b = set(companies["company_id"])
print(
    f"Number of unique company_id in companies which doesn't exist in job_postings - {len(b - a)}"
)
print(
    f"Number of unique company_id in job_postings which doesn't exist in companies - {len(a - b)}"
)
job_postings["company_id"].replace(0, np.nan, inplace=True)
print()


# cleaning job skills
print(
    f"Number of rows in job_skills before removing duplicate entries - {len(job_skills)}"
)
job_skills.drop_duplicates(inplace=True)
print(
    f"Number of rows in job_skills after removing duplicate entries - {len(job_skills)}"
)
a = set(job_postings["job_id"])
b = set(job_skills["job_id"])
print(
    f"Number of rows in job_skills before enforcing foreign key constraint - {len(job_skills)}"
)
job_skills.drop(job_skills[job_skills["job_id"].isin(b - a)].index, inplace=True)
print(
    f"Number of rows in job_skills after enforcing foreign key constraint - {len(job_skills)}"
)
print(
    f"Number of rows in job_postings that don't have any skill associated with it - {len(a - b)}"
)
print()


# cleaning job industries
print(
    f"Number of rows in job_industries before removing duplicate entries - {len(job_industries)}"
)
job_industries.drop_duplicates(inplace=True)
print(
    f"Number of rows in job_industries after removing duplicate entries - {len(job_industries)}"
)
a = set(job_postings["job_id"])
b = set(job_industries["job_id"])
print(
    f"Number of rows in job_industries before enforcing foreign key constraint - {len(job_industries)}"
)
job_industries.drop(
    job_industries[job_industries["job_id"].isin(b - a)].index, inplace=True
)
print(
    f"Number of rows in job_industries after enforcing foreign key constraint - {len(job_industries)}"
)
print(
    f"Number of rows in job_postings that don't have any industry associated with it - {len(a - b)}"
)
print()


# cleaning job benefits
print(f"Number of rows in benefits before removing duplicate entries - {len(benefits)}")
benefits.drop_duplicates(inplace=True)
print(
    f"Number of rows in job_industries after removing duplicate entries - {len(benefits)}"
)
a = set(job_postings["job_id"])
b = set(benefits["job_id"])
print(
    f"Number of rows in benefits before enforcing foreign key constraint - {len(benefits)}"
)
benefits.drop(benefits[benefits["job_id"].isin(b - a)].index, inplace=True)
print(
    f"Number of rows in benefits after enforcing foreign key constraint - {len(benefits)}"
)
print(
    f"Number of rows in job_postings that don't have any benefit associated with it - {len(a - b)}"
)
print()


# cleanup job postings data for csv export
job_postings["company_id"] = job_postings["company_id"].astype("Int64")
job_postings["applies"] = job_postings["applies"].astype("Int64")
job_postings["remote_allowed"].replace(np.nan, 0, inplace=True)
job_postings["remote_allowed"] = job_postings["remote_allowed"].astype(np.int64)
job_postings["views"] = job_postings["views"].astype("Int64")
job_postings["application_url"] = job_postings["application_url"].str.lower()
# original_listed_time has only 1 value - July 22, 2023 4:26:40 AM
job_postings["original_listed_time"] = pd.to_datetime(
    job_postings["original_listed_time"], unit="ms"
)
# expiry is only 3 values -  July 22, 2023 4:26:40 AM; November 14, 2023 10:13:20 PM; March 9, 2024 4:00:00 PM
job_postings["expiry"] = pd.to_datetime(job_postings["expiry"], unit="ms")
# closed_time has only 1 value - July 22, 2023 4:26:40 AM
job_postings["closed_time"] = pd.to_datetime(job_postings["closed_time"], unit="ms")
# listed_time has only 1 value - July 22, 2023 4:26:40 AM
job_postings["listed_time"] = pd.to_datetime(job_postings["listed_time"], unit="ms")
job_postings["description"] = (
    job_postings["description"].str.encode("utf-8").str.decode("ascii", "ignore")
)
job_postings["description"] = job_postings["description"].str.replace('"', "")
job_postings["skills_desc"] = (
    job_postings["skills_desc"].str.encode("utf-8").str.decode("ascii", "ignore")
)
job_postings["skills_desc"] = job_postings["skills_desc"].str.replace('"', "")
job_postings.replace({r"[^\x00-\x7F]+": ""}, regex=True, inplace=True)


# cleanup companies data for csv export
companies["company_id"] = companies["company_id"].astype("Int64")
companies["company_size"] = companies["company_size"].astype("Int64")
companies["state"].replace("0", np.nan, inplace=True)
companies["country"].replace("0", np.nan, inplace=True)
companies["city"].replace("0", np.nan, inplace=True)
companies["zip_code"].replace("0", np.nan, inplace=True)
companies["address"].replace("0", np.nan, inplace=True)
companies["address"].replace("-", np.nan, inplace=True)
companies["description"] = (
    companies["description"].str.encode("utf-8").str.decode("ascii", "ignore")
)
companies["description"] = companies["description"].str.replace('"', "")
companies.replace({r"[^\x00-\x7F]+": ""}, regex=True, inplace=True)


# cleanup employee counts data for csv export
employee_counts["time_recorded"] = pd.to_datetime(
    employee_counts["time_recorded"], unit="s"
)


# export csv files
job_postings.to_csv(
    "staging_data/cleaned_data/job_postings.csv",
    index=False,
    sep=",",
    escapechar="\\",
    na_rep="NULL",
    columns=[
        "job_id",
        "company_id",
        "title",
        "description",
        "skills_desc",
        "work_type",
        "location",
        "currency",
        "remote_allowed",
        "sponsored",
        "max_salary",
        "med_salary",
        "min_salary",
        "pay_period",
        "compensation_type",
        "formatted_work_type",
        "formatted_experience_level",
        "applies",
        "views",
        "original_listed_time",
        "listed_time",
        "expiry",
        "closed_time",
        "posting_domain",
        "job_posting_url",
        "application_url",
        "application_type",
    ],
)
benefits.to_csv(
    "staging_data/cleaned_data/benefits.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["job_id", "type", "inferred"],
)
job_industries.to_csv(
    "staging_data/cleaned_data/job_industries.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["job_id", "industry_id"],
)
job_skills.to_csv(
    "staging_data/cleaned_data/job_skills.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["job_id", "skill_abr"],
)
companies.to_csv(
    "staging_data/cleaned_data/companies.csv",
    index=False,
    sep=",",
    escapechar="\\",
    na_rep="NULL",
    columns=[
        "company_id",
        "name",
        "description",
        "company_size",
        "address",
        "city",
        "state",
        "country",
        "zip_code",
        "url",
    ],
)
company_industries.to_csv(
    "staging_data/cleaned_data/company_industries.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["company_id", "industry"],
)
company_specialities.to_csv(
    "staging_data/cleaned_data/company_specialities.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["company_id", "speciality"],
)
employee_counts.to_csv(
    "staging_data/cleaned_data/employee_counts.csv",
    index=False,
    quoting=csv.QUOTE_NONNUMERIC,
    columns=["company_id", "time_recorded", "employee_count", "follower_count"],
)
