select
    customer_id,
    customer_name,
    email,
    age,
    case when age <25 then 'Gen z'
        when age <40 then 'Millennial'
        when age <55 then 'Gen X'
        when age is null then 'Unknown'
        ELSE 'Boomer' END as age_segment,
    gender,
    marital_status,
    occupation,
    income_band,
    education,family_size

from {{ref('stg_users')}}