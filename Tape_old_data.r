library(DBI)
library(RPostgres)
library(readr)

# Read your CSV
csv_path <- "C:/Users/Nkoro/OneDrive - Food and Agriculture Organization/TAPE/TAPE_for_analysis_with metadata.csv"
my_data <- read_csv(csv_path)

# === Prepare long-format TAPE data for each farm with the score score ===
step2 <- my_data %>%
   select(
    instance, instance2, country, farmtype, largefarm,
    totarea, hh_total,
    area, hh_men, hh_women,
    div_score, syn_score, eff_score, rec_score,
    res_score, cultf_score, cocr_score, human_score,
    circ_score, respg_score, caet_tot, fies_score, dietary_diversity, 
    AWEAI, soil_health, tot_productivity_ha, avg_incperc
  ) %>%
  mutate(
    Survey_Year = str_extract(instance, "[0-9]{4}"),
    Project = instance %>%
      str_remove_all("[0-9]{4}") %>%
      str_replace_all("_", " ") %>%
      str_squish(),
    farm_id = paste0("farm_", ave(seq_along(instance), instance, FUN = seq_along))
  )

data <- my_data %>%
  select(
    instance, instance2, country, farmtype, largefarm,
    totarea, hh_total, area, hh_men, hh_women,
    div_score, syn_score, eff_score, rec_score,
    res_score, cultf_score, cocr_score, human_score,
    circ_score, respg_score, caet_tot, crops, animals,
    trees, div_activ, cla_int, s_plant, tree_int, connectivity,
    ext_inp, soil_fert,pest_dis, productivity, rec_biomass, 
    water, seeds_breeds, ren_energy, waste, emergingefficiency,
    stab, vuln, indebt, averdiv, diet, local_id, local_var, food_self_suff, 
    food_heritage, platforms,ae_know, partic_orgs, women, labour, youth, 
    animalwel, mkt_local, networks, local_fs, prod_empow,prod_orgs, partic_prod
  ) %>%
  mutate(
    Survey_Year = str_extract(instance, "[0-9]{4}"),
    Project = instance %>%
      str_remove_all("[0-9]{4}") %>%
      str_replace_all("_", " ") %>%
      str_squish(),
    farm_id = paste0("farm_", ave(seq_along(instance), instance, FUN = seq_along))
  ) %>%
  pivot_longer(
    cols = c(div_score, syn_score, eff_score, rec_score, res_score,
             cultf_score, cocr_score, human_score, circ_score, 
             respg_score),
    names_to = "Element",
    values_to = "Score"
  ) %>%
  mutate(
    Element = recode(Element,
      "div_score"   = "Diversity",
      "syn_score"   = "Synergy",
      "eff_score"   = "Efficiency",
      "rec_score"   = "Reciprocity",
      "res_score"   = "Resilience",
      "cultf_score" = "Culture and food",
      "cocr_score"  = "Co-Creation",
      "human_score" = "Human",
      "circ_score"  = "Circular",
      "respg_score" = "Responsible Governance"
    )
  ) %>%
  group_by(instance, farm_id) %>%
  mutate(
    caet_tot = if_else(row_number() == 1, caet_tot, 0),
    area = if_else(row_number() == 1, area, 0),
    totarea = if_else(row_number() == 1, totarea, 0)
  ) %>%
  ungroup()


# Summarise by instance (average element scores per project)@@@
tape_instance_summary <- data %>%
  group_by(instance, Survey_Year, country) %>%
  summarise(
    avg_div = mean(Score[Element == "Diversity"], na.rm = TRUE),
    avg_syn = mean(Score[Element == "Synergy"], na.rm = TRUE),
    avg_eff = mean(Score[Element == "Efficiency"], na.rm = TRUE),
    avg_rec = mean(Score[Element == "Reciprocity"], na.rm = TRUE),
    avg_res = mean(Score[Element == "Resilience"], na.rm = TRUE),
    avg_cultf = mean(Score[Element == "Culture and food"], na.rm = TRUE),
    avg_cocr = mean(Score[Element == "Co-Creation"], na.rm = TRUE),
    avg_human = mean(Score[Element == "Human"], na.rm = TRUE),
    avg_circ = mean(Score[Element == "Circular"], na.rm = TRUE),
    avg_respg = mean(Score[Element == "Responsible Governance"], na.rm = TRUE),
    avg_area = mean(area, na.rm = TRUE),
    avg_totalarea = mean(totarea, na.rm = TRUE),
    .groups = "drop"
  )%>%
  pivot_longer(
    cols = c(avg_div, avg_syn, avg_eff, avg_rec, avg_res,
             avg_cultf, avg_cocr, avg_human, avg_circ, avg_respg),
    names_to = "Element",
    values_to = "avg_score"
  ) %>%
  mutate(
    Element = recode(Element,
      "avg_div"   = "Diversity",
      "avg_syn"   = "Synergy",
      "avg_eff"   = "Efficiency",
      "avg_rec"   = "Reciprocity",
      "avg_res"   = "Resilience",
      "avg_cultf" = "Culture and food",
      "avg_cocr"  = "Co-Creation",
      "avg_human" = "Human",
      "avg_circ"  = "Circular",
      "avg_respg" = "Responsible Governance"
  )) %>%
  group_by(instance) %>%
  mutate(
    avg_area = if_else(row_number() == 1, avg_area, 0),
    avg_totalarea = if_else(row_number() == 1, avg_totalarea, 0)
  )%>%
  ungroup()

# Extract Average CAET per instance ===
AvgCAET_per_Instance <- data %>%
  select(instance, Survey_Year, country, caet_tot) %>%
  filter(caet_tot > 0) %>%   # Keep actual farm CAET scores
  group_by(instance, Survey_Year, country) %>%
  summarise(
    avg_caet = mean(caet_tot, na.rm = TRUE),
    .groups = "drop"
  )

# Extract CAET per farm ===
CAET_per_Farm <- data %>%
  filter(caet_tot > 0) %>%
  select(instance, Survey_Year, country, farm_id, caet_tot,
  caet_tot) # nolint # nolint

# Extract CAET  for each instance
indexes <- data %>%
  group_by(instance, country) %>% # nolint
  summarise(
    across( # nolint
      .cols = c( # nolint
        crops, animals, trees, div_activ, cla_int, s_plant, tree_int, connectivity, # nolint
        ext_inp, soil_fert, pest_dis, productivity, rec_biomass, water, seeds_breeds, # nolint
        ren_energy, waste, emergingefficiency, stab, vuln, indebt, averdiv, diet, # nolint
        local_id, local_var, food_self_suff, food_heritage, platforms, ae_know,
        partic_orgs, women, labour, youth, animalwel, mkt_local, networks, local_fs, # nolint
        prod_empow, prod_orgs, partic_prod
      ), # nolint
      .fns = ~ mean(.x, na.rm = TRUE)
    ) # nolint
  )%>% # nolint
  pivot_longer(
    cols = c( # nolint
        crops, animals, trees, div_activ, cla_int, s_plant, tree_int, connectivity, # nolint
        ext_inp, soil_fert, pest_dis, productivity, rec_biomass, water, seeds_breeds, # nolint
        ren_energy, waste, emergingefficiency, stab, vuln, indebt, averdiv, diet, # nolint
        local_id, local_var, food_self_suff, food_heritage, platforms, ae_know,
        partic_orgs, women, labour, youth, animalwel, mkt_local, networks, local_fs, # nolint
        prod_empow, prod_orgs, partic_prod
      ),
    names_to = "Indexes",
    values_to = "indexes score"
  ) %>%
  mutate(
    Indexes = recode(Indexes,
      "crops" = "Crop diversity",
      "animals" = "Animal rearing",
      "trees" = "Trees and agroforestry",
      "div_activ" = "Diversity of activities",
      "cla_int" = "Crop-livestock integration",
      "s_plant" = "Soil cover with plants",
      "tree_int" = "Tree integration",
      "connectivity" = "Landscape connectivity",
      "ext_inp" = "External input reduction",
      "soil_fert" = "Soil fertility",
      "pest_dis" = "Pest and disease management",
      "productivity" = "Productivity level",
      "rec_biomass" = "Biomass recycling",
      "water" = "Water management",
      "seeds_breeds" = "Seeds and breeds selection",
      "ren_energy" = "Renewable energy use",
      "waste" = "Waste management",
      "emergingefficiency" = "Energy and resource efficiency",
      "stab" = "Stability",
      "vuln" = "Vulnerability reduction",
      "indebt" = "Indebtedness risk",
      "averdiv" = "Average diversification",
      "diet" = "Diet quality",
      "local_id" = "Local identity",
      "local_var" = "Maintenance of local varieties",
      "food_self_suff" = "Food self-sufficiency",
      "food_heritage" = "Food heritage protection",
      "platforms" = "Local platforms",
      "ae_know" = "Agroecological knowledge",
      "partic_orgs" = "Participation in organizations",
      "women" = "Women empowerment",
      "labour" = "Labour sustainability",
      "youth" = "Youth inclusion",
      "animalwel" = "Animal welfare",
      "mkt_local" = "Local market access",
      "networks" = "Social networks",
      "local_fs" = "Local food systems",
      "prod_empow" = "Producer empowerment",
      "prod_orgs" = "Producer organizations",
      "partic_prod" = "Participatory production planning"
    )
  ) %>%
  select("instance", "country", "Indexes", "indexes score")# nolint



# Step 2: Connect to PostgreSQL
con <- dbConnect(
  RPostgres::Postgres(),
  dbname = "postgres",   # replace with your PostgreSQL database name
  host = "localhost",         # or your server address
  port = 5432,                # default PostgreSQL port
  user = "postgres",          # your DB username
  password = "Evende@1990"  # your DB password
)

# Step 3: Write the table to PostgreSQL (creates table automatically)
dbWriteTable(con, "Farm_score_element", data, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "Element_score_per_project", tape_instance_summary, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "Caet_score_per_project", AvgCAET_per_Instance, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "Caet_score_per_farm", CAET_per_Farm, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "indexes old", indexes, overwrite = TRUE, row.names = FALSE) # nolint
dbWriteTable(con, "step2", step2, overwrite = TRUE, row.names = FALSE) # nolint

# Step 4: Verify
dbListTables(con)  # lists all tables (should now include "tape_data")
dbGetQuery(con, "SELECT * FROM tape_data LIMIT 5;")  # preview first 5 rows

# Step 5: Disconnect
dbDisconnect(con)
