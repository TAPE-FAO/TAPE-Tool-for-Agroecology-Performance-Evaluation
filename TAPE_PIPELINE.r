library(tidyverse)
library(dplyr)
library(httr)
library(readr)
library(googlesheets4)
library(taskscheduleR)
library(KoboconnectR)
library(jsonlite)
library(DBI)
library(RPostgres)
library(readr)
library(taskscheduleR)

#setwd("C:/Users/Nkoro/TAPE_AUTOMATION") # nolint
getwd()
# Kobo data call via API with token
res <- GET(
  #"https://kobo.fao.org/api/v2/assets/a4huGZm9HowjpqDP9J8cVp/export-settings/es9etbrJvxW5gVamBvjNEzK/data.csv", # nolint
  "https://kobo.fao.org/api/v2/assets/azXtyk8FeTgD8GDySXSQs3/export-settings/estizzGVMiCEJH5HYMhdwBZ/data.csv", # nolint
  add_headers(Authorization = "4a0b6a465cb2cfb87e07eb3363d4c17d976d61da") # nolint
)

if (!dir.exists("kobo_exports")) dir.create("kobo_exports")
csv_content <- content(res, as = "text", encoding = "UTF-8")
data <- read_delim(csv_content, delim = ";") %>%
select(-c(ends_with("note")))
write_csv(data, "kobo_exports/form_test_clone.csv")

# select data and prepare to export to db
data <- data %>%
  mutate(
    Farm_id = ifelse(
      is.na(inquirer),
      paste0("Unknown_", `_id`),
      paste0(inquirer, "_", `_id`)
    )
  ) %>%
  mutate(area_agric = area_tem_crop + area_tem_meapas + area_tem_fal + area_per_crop + area_per_meapas)%>% # nolint
  select(
    Farm_id, `_uuid`, country, region, `_validation_status`,
    `_gps_loc_latitude`, `_gps_loc_longitude`, area_total, hh_or_not,
    irrig, farm_mgt, hh_men, hh_women,
    div_score, synergy_score, recycling_score, efficiency_score,
    resilience_score, cultfood_score, cocrea_score, human_score,
    circular_score, respgov_score, caet_score, area_agric, plant_diversity,temp_spatial_div, anim_diversity, # nolint
    crop_liv_aqua, habitat_mgt, seed_breed, recycling_biomass, mgt_soilfert, mgt_pestdis, water_and_energy_use, # nolint
    soc_econ_res, soil_conserve, anim_health_nutri,dietdiv_foodself, food_heritage, int_ageco,soc_know, wom_emp_caet, # nolint
    labour_socinq, labor_condition, local_market, local_circ, prod_empow, prod_access, prod_orgs       # nolint
  )

 indexes<- data %>%
  pivot_longer(
    cols = c(plant_diversity,temp_spatial_div, anim_diversity, # nolint
    crop_liv_aqua, habitat_mgt, seed_breed, recycling_biomass, mgt_soilfert, mgt_pestdis, water_and_energy_use, # nolint
    soc_econ_res, soil_conserve, anim_health_nutri,dietdiv_foodself, food_heritage, int_ageco,soc_know, wom_emp_caet, # nolint
    labour_socinq, labor_condition, local_market, local_circ, prod_empow, prod_access, prod_orgs ),
    names_to = "Indexes",
    values_to = "indexes score"
  ) %>%
  mutate(
    Indexes = recode(Indexes,
      "plant_diversity"   = "Plant Diversity",
      "temp_spatial_div"   = "Temporal and spatial diversity",
      "anim_diversity"   = "Animal diversity (including fishes and insects)",
      "crop_liv_aqua"   = "Plant-livestock-aquaculture integration",
      "habitat_mgt"   = "Habitat management",
      "seed_breed" = "Use of seeds and breeds",
      "recycling_biomass"  = "Biomass and waste management",
      "mgt_soilfert" = "Management of soil fertility",
      "mgt_pestdis"  = "Management of pests and diseases",
      "water_and_energy_use" = "Water and energy use",
      "soc_econ_res"   = "Social and economic resilience",
      "soil_conserve"   = "Soil conservation practices",
      "anim_health_nutri"   = "Animal health, nutrition, and management",
      "dietdiv_foodself"   = "Dietary diversity and food self sufficiency",
      "food_heritage"   = "Local and traditional food heritage",
      "int_ageco" = "Accessing and valuing agroecological knowledge",
      "soc_know"  = "Peer learning and co-creation of knowledge",
      "wom_emp_caet" = "Women empowerment and social equity",
      "labour_socinq"  = "Labour conditions for producers and employees",
      "labor_condition" = " Labour condition improvement",
      "local_market" = "Products and services marketed locally",
      "local_circ"  = "Local sourcing and circularity",
      "prod_empow" = "Producers' empowerment",
      "prod_access"  = "Producers' access to and control over resources",
      "prod_orgs" = "Producers' organizations and associations"
  )) %>%
  select("Farm_id", "country", "Indexes", "indexes score")

  element <- data %>%
  pivot_longer(
    cols = c(div_score, synergy_score, recycling_score, efficiency_score,
    resilience_score, cultfood_score, cocrea_score, human_score,
    circular_score, respgov_score),
    names_to = "Element",
    values_to = "avg_score"
  ) %>%
  mutate(
    Element = recode(Element,
      "div_score"   = "Div",
      "synergy_score"   = "Syn",
      "efficiency_score"   = "Eff",
      "recycling_score"   = "Rec",
      "resilience_score"   = "Res",
      "cultfood_score" = "CulFood",
      "cocrea_score"  = "Co-Cre",
      "human_score" = "Human",
      "circular_score"  = "Circu",
      "respgov_score" = "Resg"
  )) %>%
  select("Farm_id", "country", "Element", "avg_score")

# Export to postgresql db
# Connect to PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "postgres",   # nolint
  host = "localhost",         # nolint
  port = 5432,                # nolint
  user = "postgres",         # nolint
  password = "Evende@1990" # nolint
)

# Write the table to PostgreSQL (this will create the table automatically in db)
dbWriteTable(con, "TAPE_data_Kobo", data, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "TAPE+ element", element, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "TAPE+ indexes", indexes, overwrite = TRUE, row.names = FALSE) # nolint


# Verify if the table is created
dbListTables(con)  # nolint
dbGetQuery(con, "SELECT * FROM TAPE_data_Kobo LIMIT 5;")  # nolint
# Disconnect
dbDisconnect(con)

taskscheduler_create(
  taskname = "UpdateKobodatabase",
  rscript = "C:/Users/Nkoro/TAPE_AUTOMATION/TAPE_PIPELINE.R", # nolint
  schedule = "MINUTE",
  modifier = "1",
  Rexe = file.path(R.home("bin"), "Rscript.exe")
)


