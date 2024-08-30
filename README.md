# general_insurance_data_model
Module to generate artificial data to test reserving engines

## Overview
The intention of this module is to allow the generation of a simple set of data which represents a set of insurance policies. The data generated can then be used to share demonstrations of new actuarial processes, models and software.
While there is really no substitute for real data, this is often commercially sensitive, so this may be the next best thing to help share ideas and solutions within the community.


## Example of use

### Installation
```
pip install general-insurance-data-model
```

### Generating insurnace portfolio (ultimate)
The first function generates an ultimate policy and claims DataFrame:
```
import general_insurance_data_model.generators as gt
import datetime as dt

data_ultimate_m = gt.generate_ultimate_portfolio(
    class_name='Motor', 
    uw_start_date=dt.datetime.strptime('01/01/2019', '%d/%m/%Y'),
    historic_years=12,
    )
```
Other parameters can be tuned to set claim reporting and paid delays.

### filter for reporting date (reported)

There is a second function which filters based on a reporting date
- policies written after the reporting date are removed
- claims reported and paid after the reporting date are removed
```
data_reported_m = gidm.asat_filtering(
    data_ultimate_m,
    reporting_date=dt.datetime.strptime('31/1/2024', '%d/%m/%Y')
    )
```

### Optional: use with chainladder reserving package:
The policy and claims information can be imported directly into the [python-chainladder](https://chainladder-python.readthedocs.io/en/latest/intro.html) package to test different reserving methods.

```
import chainladder as cl

# build a triangle object
tri_paid = cl.Triangle(data_reported_m, 
                origin='Start_date',
                index='Class_name',
                development='Claim_payment_date',
                columns='Claim_value',
                cumulative=False).incr_to_cum().grain('OYDQ')

# plot the triangle
tri_paid.T.plot()

```

# Generator Options

generate_ultimate_portfolio has the following options

- **class_name** Default:'Class A'. Set a name for the data you are generating. This will be the name used in the Class Name Column.
- **insured_limit** Default:3000, limit of liability for the insurance contracts you are modelling
- **insured_excess** Default:250, excess amount of the insurance contracts you are modelling
- **policy_premium** Default:150, premium per policy
- **n_policies** Default:1000, number of policies to generate
- **uw_start_date** Default:datetime:'01/01/2019', first day of the underwriting year
- **historic_years=** Default:10, number of years to generate
- **historic_policy_growth** Default:0.03, when generating historic years, the number of policies will be adjusted by this number to simulate growth
- **frequency** Default:0.15, claims frequency
- **severity_mean_gu** Default:1000, average of claim amount (ground up)
- **severity_sd_gu** Default:800, standard deviation of claim amount (ground up)
- **delay_reportdays_mean** Default:100, mean number of days from start of contract until claim is reported
- **delay_reportdays_sd** Default:200, standard deviation of above
- **delay_paymentdays_mean** Default:200, mean number of days from start of contract until claim is paid
- **delay_paymentdays_sd** Default:200, standard deviation of above


## Assumptions: Policy generation
- Policies are assumed to be 12 months
- Start date of policies are uniformly distributed within 12 month period (no seasonality)
- End date of policies are 365.25 days from start date
- Three generic risk factors for the policy are generated (standard normals) [to be used to influence claim parameters]
- Option to set a policy excess and limit
- Option to set policy premium

## Assumptions: Claim generation
- Claims are assumed to arise from a single peril
- Only a single claim is generated per policy (or represeting total of policy claims). The policy is not terminated
- Frequency is set as ground-up frequency, if excess frequeny is used adjust policy excess to zero and limit as the policy exposed value
- Claim value generated from a lognormal with mean and SD
- assumed case estimate is perfect and does not change, but there is a delay to payment
- Timeings are calibrated using distibutions, offset as follows
-- Incident date is set from a uniform distribution within policy year (no seasonality)
-- Reported date is the delay in days from the incident
-- Payment date is the delay in days from the reporting of the claim

## Historic years
- You can specify the number of historic years you would like in your data set
- You can specify a growth rate for the number of policies but all other parameters are assumed to be the same for prior years


