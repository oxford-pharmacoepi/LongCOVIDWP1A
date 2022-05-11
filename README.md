# Impact of COVID-19 vaccination on preventing long COVID: A population-based cohort study using linked NHS data 

**Background:** While vaccines have shown impressive efficacy to prevent severe COVID-19, their impact on preventing long COVID is not investigated in ongoing trials.  

**Objective:** To evaluate the impact of covid vaccination on the prevention of long COVID using UK primary care data.

# Study Part 1A: Characterising Long COVID
 <img src="https://img.shields.io/badge/Study%20Status-Started-blue.svg" alt="Study Status: Started">

- Analytics use case(s): **Characterization**
- Study type: **Clinical Application**
- Tags: **OHDSI**
- Study lead: **Prof. Daniel Prieto-Alhambra, Dr. Annika Jödicke, Kristin Kostka**
- Study lead forums tag: **[daniel_prieto](https://forums.ohdsi.org/u/daniel_prieto/summary), [annika_joedicke](htvtps://forums.ohdsi.org/u/annika_joedicke)**, **[krfeeney](https://forums.ohdsi.org/u/krfeeney)**
- Study start date: **1st October 2021**
- Study end date: **TBD**
- Protocol: **To be uploaded**
- Publications: N/A
- Results explorer: [Long COVID Cohort Diagnostics App](https://dpa-pde-oxford.shinyapps.io/longcoviddiagnostics/)


## Instructions

Under Iterative Versions, there is a single R file (longCOVID-CD-11MAY2022.R) that contains the CohortDiagnostics run. The current code runs using a public WebAPI and is dependent on OHDSI libraries [CohortDiagnostics](https://github.com/ohdsi/cohortdiagnostics] (using Release v2.2.4) and [DatabaseConnector](https://github.com/OHDSI/DatabaseConnector) (using Release v5.0.2).

Note: eventually we'll have to change this to be a self-contained R package but the current approach pulls from a public WebAPI that, provided a user can access the internet, should be executable in your local environment. Updates will be made as bandwidth permits.
