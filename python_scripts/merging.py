import pandas as pd
import numpy as np


# import all the CSVs
job_postings = pd.read_csv("staging_data/cleaned_data/job_postings.csv")
companies = pd.read_csv("staging_data/cleaned_data/companies.csv")
company_industries = pd.read_csv("staging_data/cleaned_data/company_industries.csv")
company_specialities = pd.read_csv("staging_data/cleaned_data/company_specialities.csv")
employee_counts = pd.read_csv("staging_data/cleaned_data/employee_counts.csv")
benefits = pd.read_csv("staging_data/cleaned_data/benefits.csv")
job_industries = pd.read_csv("staging_data/cleaned_data/job_industries.csv")
job_skills = pd.read_csv("staging_data/cleaned_data/job_skills.csv")


# combine all address fields into address_details in companies
companies = companies[
    ["company_id", "name", "description", "company_size", "url"]
].assign(
    address_details=companies.loc[
        :, ["address", "city", "state", "country", "zip_code"]
    ].apply(
        lambda x: {
            k: v
            for k, v in x.to_dict().items()
            if (v not in [None, np.nan, {}, ""]) and (v == v)
        },
        axis=1,
    )
)


# combine company specialities to a list
print(
    f"Number of rows before combining specialities for each company to a list - {len(company_specialities)}"
)
company_specialities = (
    company_specialities.groupby("company_id").agg(lambda x: x.tolist()).reset_index()
)
company_specialities = company_specialities.rename(
    columns={"speciality": "specialities"}
)
print(
    f"Number of rows after combining specialities for each company to a list - {len(company_specialities)}"
)
print()


# combine company industries to a list
print(
    f"Number of rows before combining industries for each company to a list - {len(company_industries)}"
)
company_industries = (
    company_industries.groupby("company_id").agg(lambda x: x.tolist()).reset_index()
)
company_industries = company_industries.rename(columns={"industry": "industries"})
print(
    f"Number of rows after combining industries for each company to a list - {len(company_industries)}"
)
print()


# combine employee counts to a list
print(
    f"Number of rows before combining employee counts for each company to a list - {len(employee_counts)}"
)
employee_counts = (
    employee_counts.groupby("company_id")
    .apply(
        lambda x: x[["employee_count", "follower_count", "time_recorded"]].to_dict(
            orient="records"
        )
    )
    .reset_index(name="employee_counts")
)
print(
    f"Number of rows after combining employee counts for each company to a list - {len(employee_counts)}"
)
print()


# combine all salary fields into salary_details in job_postings
job_postings = job_postings[
    [
        "job_id",
        "company_id",
        "title",
        "description",
        "skills_desc",
        "work_type",
        "location",
        "remote_allowed",
        "sponsored",
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
    ]
].assign(
    salary_details=job_postings.loc[
        :,
        [
            "currency",
            "max_salary",
            "med_salary",
            "min_salary",
            "pay_period",
            "compensation_type",
        ],
    ].apply(
        lambda x: {
            k: v
            for k, v in x.to_dict().items()
            if (v not in [None, np.nan, {}, ""]) and (v == v)
        },
        axis=1,
    )
)


# combine job skills to a list
print(
    f"Number of rows before combining skills for each job to a list - {len(job_skills)}"
)
job_skills = job_skills.groupby("job_id").agg(lambda x: x.tolist()).reset_index()
job_skills = job_skills.rename(columns={"skill_abr": "skills"})
print(
    f"Number of rows after combining skills for each job to a list - {len(job_skills)}"
)
print()


# combine job industries to a list
print(
    f"Number of rows before combining industries for each job to a list - {len(job_industries)}"
)
job_industries = (
    job_industries.groupby("job_id").agg(lambda x: x.tolist()).reset_index()
)
job_industries = job_industries.rename(columns={"industry_id": "industry_ids"})
print(
    f"Number of rows after combining industries for each job to a list - {len(job_industries)}"
)
print()


# combine benefits to a list
print(
    f"Number of rows before combining benefits for each job to a list - {len(benefits)}"
)
benefits = (
    benefits.groupby(["job_id", "inferred"]).agg(lambda x: x.tolist()).reset_index()
)
benefits = benefits[["job_id"]].assign(
    benefits=benefits[["inferred", "type"]].apply(
        lambda x: {"inferred": x["inferred"], "types": x["type"]}, axis=1
    )
)
benefits = benefits.groupby("job_id").agg(lambda x: x.tolist()).reset_index()
print(
    f"Number of rows after combining benefits for each job to a list - {len(benefits)}"
)
print()


# merge the company tables
companies = companies.merge(company_specialities, how="left", on="company_id")
companies = companies.merge(company_industries, how="left", on="company_id")
companies = companies.merge(employee_counts, how="left", on="company_id")


# merge the job postings tables
job_postings = job_postings.merge(job_industries, how="left", on="job_id")
job_postings = job_postings.merge(job_skills, how="left", on="job_id")
job_postings = job_postings.merge(benefits, how="left", on="job_id")


# export companies json
companies.apply(
    lambda x: {
        k: v
        for k, v in x.to_dict().items()
        if (v not in [None, np.nan, {}, ""]) and (v == v)
    },
    axis=1,
).to_json("staging_data/final_collections/companies.json", orient="records")


# export job postings json
job_postings.apply(
    lambda x: {
        k: v
        for k, v in x.to_dict().items()
        if (v not in [None, np.nan, {}, ""]) and (v == v)
    },
    axis=1,
).to_json("staging_data/final_collections/job_postings.json", orient="records")
