### Incidence Rates of LOng COVID-19
remotes::install_github("darwin-eu/IncidencePrevalence")

library(DBI)
library(CDMConnector)
library(IncidencePrevalence)
library(dbplyr)
library(dplyr)
library(here)
library(ggplot2)

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
             cohort_tables = c("er_long_covid_final_cohorts"
                               ))


# parameters ---- 
study_start_date <- as.Date("01/03/2020", "%d/%m/%Y") # only needed for the gen pop denominator 
study_end_date   <- as.Date("01/09/2021", "%d/%m/%Y")  # only needed for the gen pop denominator 
study_days_prior_history <- 180  # only needed for the gen pop denominator 
study_age_stratas <- list(c(18,110))
study_age_stratas <- list(
                         c(18,39), # only adults for this study
                         c(40,64),
                         c(65,79),
                         c(80,110))
study_sex_stratas <- c("Male", "Female")

# Table in our CDM with study cohorts 
study_cohorts <- "er_long_covid_final_cohorts"
# ID cohorts 
# Cohort 9 - C9: [Long COVID] 90d prior with 14d run-in 28d after

# Id_TestNeg_cohort <- 
get_ir <- function(  table_name_strata,
                     strata_cohort_id,
                     study_age_stratas,
                     study_sex_stratas,
                     cohort_ids_outcomes){
 
  dpop <- collect_denominator_pops(
  cdm = cdm,
  study_start_date = study_start_date,
  study_end_date = study_end_date,
  study_days_prior_history = study_days_prior_history,
  table_name_strata = table_name_strata,
  strata_cohort_id = strata_cohort_id,
  study_age_stratas = study_age_stratas, 
  study_sex_stratas = study_sex_stratas,
  verbose = F)

cdm$denominator <- dpop$denominator_population
denominator_settings <- dpop$denominator_settings

## incidence rates, sex strata
inc <- collect_pop_incidence(cdm = cdm,
                             table_name_outcomes = study_cohorts ,
                             table_name_denominator = "denominator",
                             cohort_ids_outcomes = cohort_ids_outcomes,
                             cohort_ids_denominator_pops=unique(denominator$cohort_definition_id),
                             time_interval = "Months",
                             outcome_washout_windows = 0,
                             repetitive_events = F,
                             confidence_interval = "poisson",
                             minimum_cell_count = 5,
                             verbose = T
)



# get incidence estimates
incidence_estimates <- inc$incidence_estimates %>%
  left_join(denominator_settings %>% select(cohort_definition_id, age_strata, sex_strata), 
            by = c("incidence_analysis_id" = "cohort_definition_id")) %>%
  mutate(symptom_cohort = cohort_ids_outcomes)

incidence_estimates 
}


## get IR
Id_Covid_cohort   <- "22"
Id_longCov_cohort_gen <- "229990" ## incidence rates for general pop
Id_longCov_cohort <- "228990" ## this is for incidence rates among Covid infected


longCov_genpop <- get_ir(
                     table_name_strata = NULL,
                     strata_cohort_id  = NULL,
                     study_age_stratas = list(c(18,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_cohort_gen)

longCov_covid <- get_ir(
                     table_name_strata = study_cohorts,
                     strata_cohort_id  = Id_Covid_cohort,
                     study_age_stratas = list(c(18,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_cohort)


longCov_genpop_age <- get_ir(
                     table_name_strata = NULL,
                     strata_cohort_id  = NULL,
                     study_age_stratas = list(
                         c(18,39), # only adults for this study
                         c(40,64),
                         c(65,79),
                         c(80,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_cohort_gen)

longCov_covid_age <- get_ir(
                     table_name_strata = study_cohorts,
                     strata_cohort_id  = Id_Covid_cohort,
                     study_age_stratas = list(
                         c(18,39), # only adults for this study
                         c(40,64),
                         c(65,79),
                         c(80,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_cohort)

Id_tested_neg_cohort <- "30"
Id_longCov_neg_cohort_gen <- "309990" ## incidence rates for general pop
Id_longCov_neg_cohort <- "308990" ## this is for incidence rates among tested negative

test_neg_longCov_genpop <- get_ir(
                     table_name_strata = NULL,
                     strata_cohort_id  = NULL,
                     study_age_stratas = list(c(18,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_neg_cohort_gen)

longCov_covid <- get_ir(
                     table_name_strata = study_cohorts,
                     strata_cohort_id  = Id_tested_neg_cohort,
                     study_age_stratas = list(c(18,110)),
                     study_sex_stratas = study_sex_stratas,
                     cohort_ids_outcomes = Id_longCov_neg_cohort)



  
incidence_estimates <- rbind(longCov_covid, longCov_covid_age, longCov_genpop, longCov_genpop_age) %>%
  mutate(main_cohort = ifelse(symptom_cohort==229990, "General population", "COVID-19"))

# write.csv2(incidence_estimates, file(here("results", "incidence_estimates.csv"))
# 

# IR among general population ---------
### denominator pop, sex strata




# plot
fig_IR_longCov_genpop <- incidence_estimates%>% 
  filter(age_strata=="18;110")%>% 
  ggplot() +
  geom_line(aes(x =start_time, y =  ir_100000_pys, 
               #  group=categories, 
               colour=sex_strata), size = 1.3)+
  facet_grid(.~main_cohort) +
scale_y_continuous(limits = c(0,NA)) +
 scale_x_date(date_breaks = "2 month",
               date_labels = ("%b %y"),
               expand = c(0,0)) +
  scale_colour_manual(values = c( "#A20214",
                                  "#033270"  
                                    )) +
  theme_bw()+
  theme(legend.position="top")+
  ylab("IR per 100,000 p-y") +
  xlab("Date of COVID-19 infection") +
 labs(colour = "Sex")     +
  guides(colour = guide_legend(reverse=F, nrow=1))

fig_IR_longCov_genpop
# 
# 
# # denominator population, by age group 
dpop_age <- collect_denominator_pops(
  cdm_ref = cdm,
  study_start_date = study_start_date,
  study_end_date = study_end_date,
  study_days_prior_history = study_days_prior_history,
  study_age_stratas = study_age_stratas,
  study_sex_stratas = study_sex_stratas,
  verbose = T)

denominator<- dpop_age$denominator_population
denominator_settings <- dpop_age$denominator_settings

# get incidence rates, by age group
inc_age <- collect_pop_incidence(cdm_ref = cdm,
                             table_name_outcomes = study_cohorts ,
                             study_denominator_pop = denominator,
                             cohort_ids_outcomes = Id_longCov_cohort_gen,
                             cohort_ids_denominator_pops=
                               unique(denominator$cohort_definition_id),
                             time_intervals = "Months",
                             outcome_washout_windows = 0,
                             repetitive_events = F,
                             confidence_interval = "poisson",
                             minimum_cell_count = 5,
                             verbose = T
)

save(denominator,
     file = here("data", "IR_data", "general_pop_den_sex_age_strata.Rdata"))

save(incidence_estimates,
     file = here("data", "IR_data", "general_pop_IR_sex_age_strata.Rdata"))

# 
# # collect IR
incidence_estimates_age <- inc_age$incidence_estimates %>%
  left_join(denominator_settings %>% select(cohort_definition_id, age_strata, sex_strata),
            by = c("incidence_analysis_id" = "cohort_definition_id"))

# plot
fig_IR_longCov_age <- incidence_estimates_age %>%
  ggplot() +
  geom_line(aes(x =start_time, y =  ir_100000_pys,
                 colour=age_strata), size = 1.3)+
  facet_grid(.~ sex_strata)+
scale_y_continuous(limits = c(0,NA)) +
 scale_x_date(date_breaks = "2 month",
               date_labels = ("%b %y"),
               expand = c(0,0)) +
      scale_colour_manual(values = c("#033270" ,
                                     "#4091C9",
                                     "#EF3C2D",
                                     "#A20214")) +
  theme_bw()+
  theme(legend.position="top")+
  ylab("IR per 100,000 p-y") +
  xlab("Date of COVID-19 infection") +
 labs(colour = "Age group")     +
  guides(
    colour = guide_legend(reverse=F, nrow=1))

fig_IR_longCov_age


# IR among COVID-19 cases ---------
### denominator pop, sex strata
dpop <- collect_denominator_pops(
  cdm_ref = cdm,
  table_name_strata = study_cohorts ,
  strata_cohort_id = Id_Covid_cohort,
  study_sex_stratas = study_sex_stratas,
  verbose = T)

denominator <- dpop$denominator_population
denominator_settings <- dpop$denominator_settings

## incidence rates, sex strata
inc <- collect_pop_incidence(cdm_ref = cdm,
                             table_name_outcomes =study_cohorts ,
                             study_denominator_pop = denominator,
                             cohort_ids_outcomes = Id_longCov_cohort,
                             cohort_ids_denominator_pops=
                             unique(denominator$cohort_definition_id),
                             time_intervals = "Months",
                             outcome_washout_windows = 0,
                             repetitive_events = F,
                             confidence_interval = "poisson",
                             minimum_cell_count = 5,
                             verbose = T
)


incidence_estimates <- inc$incidence_estimates %>%
  left_join(denominator_settings %>% select(cohort_definition_id, age_strata, sex_strata), 
            by = c("incidence_analysis_id" = "cohort_definition_id"))


## figure
fig_IR_longCov_Cov <- incidence_estimates%>% 
  ggplot() +
  geom_line(aes(x =start_time, y =  ir_100000_pys, 
               #  group=categories, 
               colour=sex_strata), size = 1.3)+
scale_y_continuous(limits = c(0,NA)) +
 scale_x_date(date_breaks = "2 month",
               date_labels = ("%b %y"),
               expand = c(0,0)) +
  scale_colour_manual(values = c( "#A20214",
                                  "#033270"  
                                    )) +
  theme_bw()+
  theme(legend.position="top")+
  ylab("IR per 100,000 p-y") +
  xlab("Date of symptoms record") +
 labs(colour = "Sex")     +
  guides( 
    colour = guide_legend(reverse=F, nrow=1))

fig_IR_longCov_Cov

# IR among tested negative individuals ----

