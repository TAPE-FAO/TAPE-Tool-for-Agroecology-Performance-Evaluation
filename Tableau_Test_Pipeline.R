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

setwd(
  "C:/Users/Nkoro/TAPE_AUTOMATION"
)

# Kobo data call via API with token
res1 <- GET(
  "https://kobo.fao.org/api/v2/assets/ajfT4SY4zcJPVqKCJuuQxH/export-settings/eswn645vPV35hd3ULUuZgG9/data.csv", # nolint
  add_headers(Authorization = "4a0b6a465cb2cfb87e07eb3363d4c17d976d61da") # nolint
)

if (!dir.exists("kobo_exports")) dir.create("kobo_exports")
csv_content <- content(res1, as = "text", encoding = "UTF-8")
data <- read_delim(csv_content, delim = ";") %>%
  select(-c(ends_with("note")))


#preview data
#res2 <- GET(
#"https://kobo.fao.org/api/v2/assets/a4huGZm9HowjpqDP9J8cVp/export-settings/es9etbrJvxW5gVamBvjNEzK/data.csv", # nolint
#"https://kobo.fao.org/api/v2/assets/aJk2PpYpucUv7cd6bcxAYa/export-settings/esiWcp4Auad97N62S4rpkvK/data.csv", # nolint
#add_headers(Authorization = "4a0b6a465cb2cfb87e07eb3363d4c17d976d61da") # nolint
#)
#res2
#if (!dir.exists("kobo_exports")) dir.create("kobo_exports")
#csv_content <- content(res2, as = "text", encoding = "UTF-8")
#data2 <- read_delim(csv_content, delim = ";") %>%
#select(-c(ends_with("note")))

#common_cols <- intersect(names(data1), names(data2))

#for (col in common_cols) {
# if (class(data1[[col]])[1] != class(data2[[col]])[1]) {
#  data1[[col]] <- as.character(data1[[col]])
# data2[[col]] <- as.character(data2[[col]])
# }
#}

#combine data
#data <- bind_rows(data1, data2)
#write_csv(data, "kobo_exports/form_test_clone1.csv")

#data <- read.csv("kobo_exports/form_test_clone1.csv")
#view(data)


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
    Farm_id, country, region,
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
  dbname = "tape_trials_2026",   # nolint
  host = "34.34.103.150",         # nolint
  port = 5432,                # nolint
  user = "dave_nkoro",         # nolint
  password = "awyd_RtDPJp6!eDr" # nolint
)

# Write the table to PostgreSQL (this will create the table automatically in db)
dbWriteTable(con, "TAPE_data_Kobo", data, overwrite = TRUE, row.names = FALSE)
dbWriteTable(con, "TAPE+ element", element, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "TAPE+ indexes", indexes, overwrite = TRUE, row.names = FALSE) # nolint

# update internal statistics to improve performance
dbExecute(con, "ANALYZE;")

# This will refresh extracts and update data automatically.

tableau_cmd <- paste0(
  '"C:/Program Files/Tableau/Tableau 2023.3/bin/tableau.exe" ',
  '-refresh ',
  '--workbook "C:/Users/Nkoro/OneDrive - Food and Agriculture Organization/TAPE/TAPE DASHBOARDS/test.twbx"'
)

system(tableau_cmd, wait = TRUE)
cat("✅ Tableau Desktop extract refreshed successfully.\n")

# Verify if the table is created
dbListTables(con) # nolint
#dbGetQuery(con, "SELECT * FROM TAPE_data_Kobo LIMIT 5;")  # nolint
# Disconnect
dbDisconnect(con)


taskscheduler_create(
  taskname  = "UpdateKobodatabase",
  rscript   = "C:/Users/Nkoro/TAPE_AUTOMATION/TAPE_PIPELINE.R",
  schedule  = "MINUTE",
  modifier  = 1,                             
  starttime = format(Sys.time() + 60, "%H:%M"),
  startdate = format(Sys.Date(), "%m/%d/%Y"),
  Rexe      = file.path(R.home("bin"), "Rscript.exe")
)


