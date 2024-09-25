{{
    config(
        materialized='table'
    )
}}

{% if var('mode') == 'demo_no_records_in_es_nodes_error' %}
   select null as batch_id
{% elif var('mode') == 'demo_records_in_es_nodes_error' %}
   select 1 as batch_id
{% else %}

select BATCH_ID,
       URL,
       RISK_EXPOSURE_INDEX,
       NATURAL_DISASTER_INDEX,
       DEACTIVATED,
       POSTAL_CODE,
       STREET,
       INCIDENT_ALERTING_PROFILE,
       LONGITUDE,
       LATITUDE,
       CITY,
       NODE_TYPE,
       NODE_NAME,
       COUNTRYISO3,
       EXTERNAL_ID,
       SUB_SECTORS,
       FLOOD,
       FLASH_FLOOD,
       WAR,
       TERRORISM,
       CIVIL_UNREST,
       OPERATIONAL_INDEX,
       POLITICAL_VIOLENCE_INDEX,
       SOCIO_POLITICAL_INDEX,
       SUSTAINABILITY_INDEX,
       RISK_TO_INDIVIDUALS_INDEX,
       EARTHQUAKE,
       TORNADO,
       VOLCANO,
       EXTRATROPICAL_CYCLONE,
       HAIL,
       TROPICAL_CYCLONE,
       LIGHTNING,
       WILDFIRE,
       TSUNAMI,
       STORM_SURGE,
       CORRUPTION,
       LAW_ENFORCEMENT,
       LABOUR_STRIKE,
       COUNTERFEIT_THREAT,
       AVIATION,
       GROUND,
       MARINE,
       CUSTOMS_EFFICIENCY,
       CARGO_THEFT,
       PERSONAL_FREEDOM,
       WORKERS_RIGHTS,
       CHILD_LABOUR,
       ENVIRONMENT,
       DEATH_AND_INJURY,
       DETENTION_AND_DISCRIMINATION,
       KIDNAP_AND_RANSOM,
       SUPPLY_WATCH_COMPANY_NAME,
       ERROR,
       LOAD_DATE

from   {{ref('es_nodes_temp')}}
where  error is null

{% endif %}