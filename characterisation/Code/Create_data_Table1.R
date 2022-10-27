##  CHaracteristing Long COVID ### 
# Getting  TABLE 1-  Comparing characteristics of Covid cases vs Long Covid 

# packages
library(SqlRender)
library(DBI)
library(DatabaseConnector)
library(here)
library(dplyr)
library(tidyr)
library(CDMConnector)
library(stringr)
library(lubridate)
library(tableone)


# Connection details ----
server    <- Sys.getenv("SERVER_JUN22")
server_dbi<- Sys.getenv("SERVER_DBI_JUN22")
user      <- Sys.getenv("DB_USER_JUN22")
password  <- Sys.getenv("DB_PASSWORD_JUN22")
port      <- Sys.getenv("DB_PORT_JUN22") 
host      <- Sys.getenv("DB_HOST_JUN22") 


db <- dbConnect(RPostgres::Postgres(), 
                dbname = server_dbi,
                port = port,
                host = host, 
                user = user, 
                password = password)

targetDialect              <- "postgresql"
cdm_database_schema        <- "omop21t4_cmbd" 
vocabulary_database_schema <-"omop21t4_cmbd" 
write_schema               <-"results21t4_cmbd"

# Generate cdm object to access the tables
cdm <- cdm_from_con(db, 
             cdm_schema = cdm_database_schema,
             write_schema = write_schema,
             cohort_tables = c("er_long_covid_final_cohorts"))

cohorts_ids  <- read.csv2(here("ATLAS Cohort Definitions_LongCovid.csv")) %>%
                          mutate(name= paste(sub(".*LongCov-ER_", "", name)))


## cohrot of interest - first New Covid infections
# main_cohort_interest <- cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == 22)
# 90 days long Covid cohort
# first two digits are for the "mother cohort"
# digits 3,4 are for the symptoms cohort (99 is for any symptom") 
# last two digits for window period
# long_covid_cohort <- cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == 229990)


# Function to get the data for the table 
generate_data_table1 <- function(main_cohort_interest, long_covid_cohort){
working_cohort <- main_cohort_interest %>% 
 rename(person_id=subject_id) %>%
  left_join(cdm$person  %>%
              select(person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth) %>%
              mutate(gender= ifelse(gender_concept_id==8507, "Male",
                             ifelse(gender_concept_id==8532, "Female", NA ))) %>%
              select(-gender_concept_id)) %>%
  # SIDIAP specific ----
 left_join(cdm$observation %>%
             filter(observation_source_value == "medea") %>% 
             select(person_id, observation_source_value, value_as_string) %>%
             filter(value_as_string %in% c("R", "U1", "U2", "U3", "U4", "U5")) %>%
             mutate(medea=value_as_string) %>%
             select(person_id, medea)) %>%
   left_join(cdm$observation %>%
             select(person_id, observation_source_value, value_as_string) %>%
             filter(observation_source_value == "agr_nationality") %>%
            rename(nationality = value_as_string) %>%
             select(person_id, nationality))%>%
  compute()
         
  # we get age - SIDIAP specific? ----
    working_cohort<- working_cohort %>%
    mutate(dob = as.Date(paste0(year_of_birth, "-",
                                month_of_birth, "-",
                                day_of_birth))) %>%
    mutate(age= floor((cohort_start_date - dob) /365.25)) %>%
    select(-year_of_birth, -month_of_birth, -day_of_birth, -dob) %>%
      mutate(age_gr2=ifelse(age<=34,  "<=34",
                 ifelse(age>=35 & age<=49,  "35-49",    
                 ifelse(age>=50 & age<=64,  "50-64",    
                 ifelse(age>=65 & age<=79,  "65-79",    
                 ifelse(age>=80, ">=80",
                       NA)))))) %>% 
    compute() 

# clean nationality   # SIDIAP specific ----
 working_cohort <- working_cohort %>%
   mutate(nationality = ifelse(str_detect(nationality, "Austr"), "Australia & New Zealand", nationality)) %>%
   mutate(nationality_cat = ifelse( nationality== "Espanya", "Spain", 
                            ifelse(str_detect( nationality, "frica"),  "Africa",  
                            ifelse(str_detect( nationality, "rica central"), "Central & South America",
                            ifelse(str_detect( nationality, "del Nord"), "Europe & North America",    
                            ifelse(str_detect( nationality, "Europa"), "Europe & North America",       
                            ifelse(str_detect( nationality, "Europa oriental"),  "Eastern Europe",       
                            ifelse(str_detect( nationality, "del Sud"), "Central & South America",      
                            ifelse(str_detect( nationality, "Carib"), "Central & South America", 
                            ifelse(str_detect( nationality, "sia"), "Asia & Oceania",
                            ifelse(str_detect( nationality, "Melan"), "Asia & Oceania", 
                            ifelse(str_detect( nationality, "Polin"), "Asia & Oceania",
                            ifelse(str_detect( nationality, "Micro"), "Asia & Oceania",
                            ifelse(str_detect( nationality, "Ant"),"Asia & Oceania", 
                            ifelse(str_detect( nationality, "Australia & New Zealand"), "Asia & Oceania",
                                   NA))))))))))))))) %>%
   select(-nationality) %>%
   compute()    
 #### Comorbidities
 # get comorbidities at baseline
 # looking at only records on or prior to specified date
autoimmune_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==434621) %>% 
   collect()
 
 autoimmune_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!autoimmune_disease.codes$descendant_concept_id) %>% 
   full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 
 asthma.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==317009) %>% 
   collect()
 
 asthma <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!asthma.codes$descendant_concept_id) %>% 
   full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 
 malignant_neoplastic_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==443392) %>% 
   collect()
 
 malignant_neoplastic_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!malignant_neoplastic_disease.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
     compute() 
  
 
 diabetes.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==201820) %>% 
   collect()
 
 diabetes <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!diabetes.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
      compute() 
 
 
 heart_disease.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==321588) %>% 
   collect()
 
 heart_disease <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!heart_disease.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 
 hypertensive_disorder.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==316866) %>% 
      collect() 
 
 
 hypertensive_disorder <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!hypertensive_disorder.codes$descendant_concept_id) %>%
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
  
 renal_impairment.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==4030518) %>% 
   collect()
 
 renal_impairment <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!renal_impairment.codes$descendant_concept_id) %>%
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute() 
 
 copd.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==255573) %>% 
   collect()
 
 copd <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!copd.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute()
 
 
 dementia.codes<- cdm$concept_ancestor %>% 
   filter(ancestor_concept_id ==4182210) %>% 
   collect()
 
 dementia <- cdm$condition_occurrence %>% 
   filter(condition_concept_id %in% !!dementia.codes$descendant_concept_id) %>% 
      full_join(working_cohort %>% select(person_id, cohort_start_date)) %>%
   filter(condition_start_date < cohort_start_date) %>%
   select(person_id) %>% 
   distinct() %>% 
   compute()
 
 
 rm(autoimmune_disease.codes,
    asthma.codes,
    malignant_neoplastic_disease.codes,
    diabetes.codes,
    heart_disease.codes,
    hypertensive_disorder.codes,
    renal_impairment.codes,
    copd.codes,
    dementia.codes)
 
 
 ## Add to exposure pop
working_cohort <- working_cohort %>%
   left_join(asthma %>% mutate(asthma = 1)) %>%
   left_join(autoimmune_disease %>% mutate(autoimmune_disease = 1)) %>%
   left_join(copd %>% mutate(copd = 1 )) %>%
   left_join(dementia %>% mutate(dementia = 1)) %>%
   left_join(diabetes %>% mutate(diabetes = 1)) %>%
   left_join(heart_disease %>% mutate(heart_disease = 1)) %>%
   left_join(hypertensive_disorder %>% mutate(hypertension = 1)) %>%
   left_join(malignant_neoplastic_disease %>% mutate(cancer = 1)) %>%
   left_join(renal_impairment %>% mutate(renal_impairment = 1)) %>%
   mutate(asthma=ifelse(is.na(asthma),0,asthma)) %>% 
   mutate(autoimmune_disease=ifelse(is.na(autoimmune_disease),0,autoimmune_disease)) %>% 
   mutate(copd=ifelse(is.na(copd),0,copd)) %>% 
   mutate(dementia=ifelse(is.na(dementia),0,dementia)) %>% 
   mutate(diabetes=ifelse(is.na(diabetes),0,diabetes)) %>% 
   mutate(heart_disease=ifelse(is.na(heart_disease),0,heart_disease)) %>% 
   mutate(hypertension=ifelse(is.na(hypertension),0,hypertension)) %>% 
   mutate(cancer=ifelse(is.na(cancer),0,cancer)) %>% 
   mutate(renal_impairment=ifelse(is.na(renal_impairment),0,renal_impairment)) 
 
 
 rm(autoimmune_disease,
    asthma,
    copd,
    dementia,
    diabetes,
    heart_disease,
    hypertensive_disorder,
    malignant_neoplastic_disease,
    renal_impairment)
 
vaccinations <- cdm$drug_exposure%>% 
      filter(drug_concept_id %in% 
           c("37003436","724905",  "37003518", "739906")) %>% 
  full_join(main_cohort_interest %>% rename(person_id=subject_id)) %>%
  filter(drug_exposure_start_date < cohort_start_date) %>%
  compute()  %>%
   #"BNT162b2" #"ChAdOx1" #"mRNA-1273" #Ad26.COV2.S
   select(person_id, drug_concept_id, drug_exposure_start_date) %>%
  distinct() %>%
   mutate(vaccine.type=
            ifelse(drug_concept_id=="37003436", "BNT162b2",
                   ifelse(drug_concept_id=="724905", "ChAdOx1",
                          ifelse(drug_concept_id=="37003518", "mRNA-1273",
                                 ifelse(drug_concept_id=="739906", "Ad26.COV2.S",NA ))))) %>%
  compute()
 
# we get  the dates and vaccines types for the different doses
vaccines <- vaccinations %>%
      arrange(person_id,drug_exposure_start_date) %>% 
      group_by(person_id) %>% 
      mutate(seq=row_number())   %>%
      ungroup () %>%
      group_by(person_id) %>%
      arrange(person_id,drug_exposure_start_date) %>% 
  mutate(second_dose_type = ifelse(seq==1, 
                                   lead(vaccine.type, order_by = c("drug_exposure_start_date")), 
                                   NA),
             second_dose_date = ifelse(seq==1,
                                  lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")), 
                                  NA)) %>%
  ungroup() %>%
  compute()

vaccines %>% select(seq) %>% distinct()

vaccines <- vaccines %>%
  filter(seq!=2) %>%
  group_by(person_id) %>%
      arrange(person_id,drug_exposure_start_date) %>% 
  mutate( third_dose_type = ifelse(seq==1,
                                   lead(vaccine.type, order_by = c("drug_exposure_start_date")), 
                                   NA),
             third_dose_date = ifelse(seq==1,
                                  lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")),
                                  NA)) %>%
  ungroup() %>%
  filter(seq!=3) %>%
  group_by(person_id) %>%
    arrange(person_id,drug_exposure_start_date) %>% 
  mutate(fourth_dose_type = ifelse(seq==1,
                                   lead(vaccine.type, order_by = c("drug_exposure_start_date")), 
                                   NA),
             fourth_dose_date = ifelse(seq==1,
                                       lead(drug_exposure_start_date, order_by = c("drug_exposure_start_date")),     
                                  NA)) %>%
  ungroup() %>%
  filter(seq!=4) %>%
  rename(first_dose_type = vaccine.type,
         first_dose_date = drug_exposure_start_date) %>%
  select(-drug_concept_id, -seq) %>%
  mutate(one_dose_vaccine = ifelse(is.na(first_dose_date), 0, 1)) %>%
  mutate(fully_vaccinated = ifelse(first_dose_type=="Ad26.COV2.S", 1,
                                   ifelse(is.na(second_dose_date), 0,1))) %>%
  mutate(booster_doses = ifelse(first_dose_type=="Ad26.COV2.S" & !(is.na(second_dose_date)),  1,
                                ifelse(is.na(third_dose_date), 0,1))) %>%
  mutate(vaccination_status = ifelse(booster_doses==1, "Received booster doses", 
                              ifelse( fully_vaccinated ==1, "Primary vaccination",
                              ifelse( one_dose_vaccine == 1, "First-dose vaccination", NA)
                              ))) %>%
  compute()

 
working_cohort <- working_cohort%>%
  left_join(vaccines)%>% 
  mutate(vaccination_status = ifelse(is.na(vaccination_status), "Non vaccinated", vaccination_status)) %>%
                                     compute()

 ## we add long covid 90 days 
 working_cohort <- working_cohort %>%
   left_join( long_covid_cohort   %>% 
                select(-cohort_definition_id) %>%
                mutate(long_covid = 1) %>%
                rename(long_covid_date = cohort_end_date,
                      person_id = subject_id))  %>%
    mutate(long_covid=ifelse(is.na(long_covid),0,long_covid)) %>%
   distinct() %>%
 collect()
 
working_cohort
}


# Get ids for cohorts of interest 
# New infection
main_cohort_id <- cohorts_ids %>%
                    filter(str_detect(name, "New_Infection")) %>% 
                    select(cohort_definition_id) %>% pull()
# Symptoms - long Covid is 99
symptom_id <- 99
# time window of interest
window_id <- 90

long_covid_id <- main_cohort_id*10^4+symptom_id*10^2+window_id


table1_data_new_infections <- generate_data_table1(
  main_cohort_interest = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == main_cohort_id) ,
  long_covid_cohort = cdm$er_long_covid_final_cohorts %>% filter(cohort_definition_id == long_covid_id))

save(table1_data_new_infections,
     file = here("data", "table1_data_new_infections.Rda"))

