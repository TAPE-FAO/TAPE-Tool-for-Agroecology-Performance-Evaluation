library(tidyverse)
library(dplyr) # Explicitly import dplyr for rowwise
library(httr)
library(readr)
library(googlesheets4)
library(taskscheduleR)
library(KoboconnectR)
library(jsonlite)

#credentials
url = "eu.kobotoolbox.org"
uname = "faoagroecology"
pwd = "agroecologyfao2019" # nolint

# Kobo API call with token
token <- get_kobo_token(url, uname, pwd)

res <- GET(
  "https://eu.kobotoolbox.org/api/v2/assets/ap23T8XfRHLk7yC8bYt98L/export-settings/esAqxw2iK2zemrSVrSYJcVK/data.csv", # nolint
  add_headers(Authorization = paste("Token", token))
)
if (!dir.exists("kobo_exports")) dir.create("kobo_exports")
csv_content <- content(res, as = "text", encoding = "UTF-8")
data <- read_delim(csv_content, delim = ";") %>%
select(-c(ends_with("note")))
write_csv(data, "kobo_exports/form_test_clone.csv")


data <- data %>%
  select(`_id`, `_uuid`, `country`,region,`_validation_status`,
  `area_total`, `hh_or_not`, # nolint: indentation_linter.
  `irrig`,`farm_mgt`,`hh_men`,`hh_women`,
  `div_score`,`synergy_score`,`recycling_score`,`efficiency_score`, 
   `resilience_score`, `cultfood_score`,`cocrea_score`,`human_score`, # nolint: indentation_linter, commas_linter, line_length_linter.
   `circular_score`, `respgov_score`, `caet_score`) # nolint: line_length_linter.


#### Diversity
##The Diversity score is calculated by the sum of the amount of **cultivated crops**[score 0-4], amount of **tree species** [score 0-4], amount of **animal species** [score 0-4] and amount of **income producing activities** (div_activ) score [0-4]. this sum is then divided by 16 and multiplied by 100. # nolint: line_length_linter.
caet_diversity <- function(data) { # nolint
  Diversity <- data %>% # nolint
    rowwise() %>%
    mutate(
      # Plant diversity
      sum_plant_diversity = num_plant + plant_gendiv,
      plant_div_score = round((sum_plant_diversity / 4) * 100, 2),

      # Temporal + spatial diversity
      sum_temp_spatial_div = temp_div + space_div,
      temp_spatial_score = round((sum_temp_spatial_div / 4) * 100, 2),

      # Animal diversity (only if raise_animals == 1)
      sum_anim_diversity = if_else(raise_animals == 1,
                                 num_animal + anim_gendiv,
                                 NA_real_),
      animal_div_score = if_else(raise_animals == 1,
                                 round((sum_anim_diversity / 4) * 100, 2),
                                 NA_real_),

      # Combined diversity index (safely handles missing animal_diversity)
      div_score = round(mean(c(plant_diversity, temp_spatial_div, animal_diversity), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Diversity) # nolint
}

### Synergies
##The Synergy score is calculated by the sum of the **crop-animal-integration** (cla_int) score [0-4], the **soil-plant-management** score [0-4], the **integration of trees** [score 0-4] and a score for the **connectivity of the farm to the landscape** [0-4]. This sum is then divided by 16 and multiplied by 100. # nolint
caet_synergies <- function(data) { # nolint
  Synergies <- data %>% # nolint
    rowwise() %>%
    mutate(
      # crop, livestock and aquaculture synergy (only if raise_animals == 1) # nolint
      sum_crop_liv_aqua = if_else(raise_animals == 1,
                                 feed_prod + vary_service + tree_farm, # nolint
                                 NA_real_),
      crop_liv_aqua_score = if_else(raise_animals == 1, # nolint
                                 round((sum_crop_liv_aqua / 4) * 100, 2), # nolint
                                 NA_real_),
      # habitat_mgt # nolint
      sum_habitat_mgt = plot_mosaic + natveg_pct,
      habitat_mgt_score = round((sum_habitat_mgt / 4) * 100, 2),
      # Combined synergy index (safely handles missing synergies related to livestock) # nolint
      syn_score = round(mean(c(crop_liv_aqua, habitat_mgt), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()
     # nolint
  return(Synergies) # nolint
}

diversity_results <- caet_synergies(data)
write_csv(diversity_results, "kobo_exports/synergies_scores.csv")


### Recycling
##The Recycling score is calculated by the sum of **recycled biomass** [score 0-4], the **waste production and management** score [0-4], the **water recycling & saving** score [0-4], and **energy reduction and** **renewable energy** score [0-4]. This sum is then divided by 16 and multiplied by 100. # nolint
caet_recycling <- function(data) { # nolint
  Recycling <- data %>% # nolint
    rowwise() %>%
    mutate(
      # seed and breed # nolint
      sum_seed_breed = useloc_seedbreed + nat_seedbreed,
      seed_breed_score = round((sum_seed_breed / 4) * 100, 2),

      # recycling + biomass # nolint
      sum_recycling_biomass = reuse_orgres + nonorg_waste + plastic_waste,
      recycling_biomass_score = round((sum_recycling_biomass / 6) * 100, 2),

      # Water energy (only if source_irrig !="") # nolint
      water_energy_score = if_else(!is.na(source_irrig) & source_irrig != "", # nolint
                                 ((source_irrig + water_save + renew_energy) / 6) * 100, # nolint
                                 ((water_save + renew_energy) / 4 * 100))  , # nolint
                                  # nolint
      # Combined Recycling index
      rec_score = round(mean(c(seed_breed_score, recycling_biomass_score, water_energy_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Recycling) # nolint
}


### Efficiency
##  The Efficiency score is calculated by the sum of the **use of external inputs** [score 0-4], **Management of soil fertility** [score 0-4], **Management of pests and diseases** [score 0-4] and by a score of e**merging efficiency from good practices** [score 0-4]. This sum is the divided by 16 and multiplied by 100. # nolint

caet_efficiency <- function(data) { # nolint
  Efficiency <- data %>% # nolint
    rowwise() %>%
    mutate(
      # mgt_soilfert # nolint
      sum_mgt_soilfert = onfarm_nutcycle + nitrogen_plant,
      mgt_soilfert_score = round((sum_mgt_soilfert / 4) * 100, 2),

      # mgt_pestdis # nolint
      sum_mgt_pestdis =  if_else(!is.na(use_vet) & use_vet != "",
                  onf_eco + use_vet, # nolint
                  onf_eco),
      sum_mgt_pestdis_score = if_else(!is.na(use_vet) & use_vet != "",
                 round((sum_mgt_pestdis / 4) * 100, 2), # nolint
                 round((sum_mgt_pestdis / 2) * 100, 2)),

      # resource_optim # nolint
      sum_resource_optim = align_prodseason + posthloss_tech,
      resource_optim_score =  round((sum_resource_optim / 4) * 100, 2), # nolint
      # Combined efficiency index
      eff_score = round(mean(c(mgt_soilfert_score, sum_mgt_pestdis_score, resource_optim_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Efficiency) # nolint
}


### Resilience
##The Resilience score is calculated by the sum of **Existence of social mechanisms to reduce vulnerability** (vuln) [score 0-4], the **Environmental resilience and capacity to adapt to climate change** (indebt) [score 0-4], the mean of the 4 diversity (div_aver) scores, and the average score of the **indices measuring self-sufficiency and empowerment** (aver_suss_emp). The sum is then divided by 16 and multiplied by 100. # nolint

caet_resilience <- function(data) { # nolint
  Resilience <- data %>% # nolint
    rowwise() %>%
    mutate(
      # soc_econ_res # nolint
      sum_soc_econ_res = community_coop + ext_finance + div_activ,
      soc_econ_res_score = round((sum_soc_econ_res / 6) * 100, 2),

      # soil_conserve # nolint
      soil_conserve_score=  round(((soil_cover + soil_disturb + soil_cons) / 6) * 100, 2), # nolint

      #anim_welfare
      sum_anim_welfare = if_else(raise_animals == 1,
                                 anim_health + anim_mgt, # nolint
                                 NA_real_),
      anim_health_nutri_score = if_else(raise_animals == 1,
                                 round((sum_anim_welfare / 4) * 100, 2), # nolint
                                 NA_real_),                           # nolint

      # Combined resilience index
      res_score = round(mean(c(soc_econ_res_score, soil_conserve_score, anim_health_nutri_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Resilience) # nolint
}


### Culture and food traditions
##The Culture and food traditions score is calculated by the sum of a score for **nutritional diet** (diet) [score 0-4], the score for **food self-sufficiency** (food_self_suff) [score 0-4], the **Local and traditional food heritage** (food_heritage) [score 0-4] and the **management of seeds and breeds** (seeds_breeds) [score 0-4]. The sum is then divided by 16 and multiplied by 100. # nolint

caet_culture <- function(data) { # nolint
  Culture <- data %>% # nolint
    rowwise() %>%
    mutate(
      # dietdiv_foodself# nolint
      sum_dietdiv_foodself = food_div + farm_ingred,
      dietdiv_foodself_score = round((sum_dietdiv_foodself / 4) * 100, 2),

      # food heritage # nolint
      sum_food_heritage =  trad_food + cult_activ + value_heritage, # nolint
      food_heritage_score = round((sum_food_heritage / 6) * 100, 2),                       # nolint

      # Combined culture index
      cultf_score = round(mean(c(dietdiv_foodself_score, food_heritage_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Culture) # nolint
}


### Co-creation
##The Co-creation score is calculated by the sum of the **availability of social platforms for knowledge transfer** (platforms)[score 0-4], **access and interest to agroecological knowledge** (ae_know) [score 0-4] and the **interconnection of producers to their community and grassroot networks** (partic_orgs) [score 0-4]. This sum is then divided by 12 and multiplied by 100. # nolint

caet_cocreation <- function(data) { # nolint
  Cocreation <- data %>% # nolint
    rowwise() %>%
    mutate(
      # int_ageco# nolint
      sum_int_ageco = context_know + share_aginnov + div_know,
      int_ageco_score = round((sum_int_ageco / 6) * 100, 2),

      # social knowledge # nolint
      sum_soc_know =  know_platform + intergen + know_process, # nolint
      soc_know_score = round((sum_soc_know / 6) * 100, 2),                       # nolint

      # Combined cocreation index
      cocr_score = round(mean(c(int_ageco_score, soc_know_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Cocreation) # nolint
}


### Human and social values
##The score for human and social values is calculated by the sum of the **women empowerment** (women) score [0-4], a score that measures **working conditions, wages and family participation** (labour) [score 0-4], a score of weather the **youth wants to continue farming** (youth) [0-4] and a **animal welfare** (animalwel) score [0-4]. Then the sum is divided by 16 (or 12 if there are no animals on the farm) and multiplied by 100. # nolint

caet_humanvalues <- function(data) { # nolint
  Humanvalues <- data %>% # nolint
    rowwise() %>%
    mutate(
      # wom_emp_caet # nolint
      sum_wom_emp_caet = women_role + youth + school_child,
      wom_emp_caet_score = round((sum_wom_emp_caet / 6) * 100, 2),

      # labour_socinq # nolint
      sum_labour_socinq = ifelse(
                          workcond_emp != "", # nolint
                          workcond_farmer + workcond_emp + agro_innov, # nolint
                          workcond_farmer + agro_innov), # nolint
      labour_socinq_score = ifelse(
                          workcond_emp != "", # nolint
                          round((sum_labour_socinq / 6) * 100, 2),
                          round((sum_labour_socinq / 4) * 100, 2)),                       # nolint
      # labor_condition                    # nolint
      sum_labor_condition = motiv_farm + activ_outside,
      labor_condition_score = round((labor_condition_score / 4) * 100, 2),
       # nolint
      # Combined humanvalues index
      human_score = round(mean(c(wom_emp_caet_score, labour_socinq_score, labor_condition_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Humanvalues) # nolint
}

### Circular economy and solidarity
##The score for circular economy and solidarity is calculated by the sum of a score for **local marketing** (mkt_local) [score 0-4], **relationship with the consumers** (networks) [score 0-4] and and a score for the **local food system** (local_fs) [score 0-4]. The sum is then divided by 12 and multiplied by 100. # nolint

caet_circular <- function(data) { # nolint
  Circular <- data %>% # nolint
    rowwise() %>%
    mutate(
      # local market # nolint
      sum_local_market = local_market_share + pgs_cert,
      local_market_score = round((sum_local_market / 4) * 100, 2),

      # local_circ # nolint
      sum_local_circ =  orig_inputs + local_process + resource_share, # nolint
      local_circ_score = round((sum_local_circ / 6) * 100, 2),                       # nolint

      # Combined Circular index
      circ_score = round(mean(c(local_market_score, local_circ_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Circular) # nolint
}



### Responsible Governance
##The score for responsible governance is calculated by the sum of the **producers rights and bargaining power** [score 0-4], **the cooperation between producers** [score 0-4] and the **possibility to participate in land governance** [score 0-4]. The sum is then divided by 12 and multiplied by 100. # nolint

caet_governance <- function(data) { # nolint
  Governance <- data %>% # nolint
    rowwise() %>%
    mutate(
      # prod_empow # nolint
      sum_prod_empow = auto_dec + access_finance + access_market,
      prod_empow_score = round((sum_prod_empow / 6) * 100, 2),

      # prod_access # nolint
      sum_prod_access =  access_land + access_water + access_gene, # nolint
      prod_access_score = round((sum_prod_access / 6) * 100, 2),                       # nolint

      # prod_orgs # nolint
      sum_prod_orgs =  engage_org + part_decision + farm_inv, # nolint
      prod_orgs_score = round((sum_prod_orgs / 6) * 100, 2),                       # nolint

      # Combined governance index
      respg_score = round(mean(c(prod_empow_score, prod_access_score, prod_orgs_score), na.rm = TRUE), 2) # nolint
    ) %>%
    ungroup()

  return(Governance) # nolint
}


#diversity_results <- caet_efficiency(data)
#write_csv(diversity_results, "kobo_exports/recycling_scores.csv")



### Caracterisation of Agroecological Transition (CAET)
##To calculate the Characterization of agroecological transition (CAET) the mean score of all the above calculated scores
##(Diversity, Synergies, Efficiency, Recycling, Resilience, Culture and food traditions, Co-creation, Human and social 
##values and Responsible Governance) is calculated.

caet_CAET <- function(){
  Diversity <- caet_diversity()
  Synergies <- caet_synergies()
  Efficiency <- caet_efficiency()
  Recycling <- caet_recycling()
  Resilience <- caet_resilience()
  Culture <- caet_culture()
  Cocreation <- caet_cocreation()
  Humanvalues <- caet_humanvalues()
  Circular <- caet_circular()
  Governance <- caet_governance()
  
  CAET <- Diversity %>%
    select(key, div_score) %>%
    left_join(Synergies[,c('key', 'syn_score')], by = 'key') %>%
    left_join(Efficiency[,c('key', 'eff_score')], by = 'key') %>%
    left_join(Recycling[,c('key', 'rec_score')], by = 'key') %>%
    left_join(Resilience[,c('key', 'res_score')], by = 'key') %>%
    left_join(Culture[,c('key', 'cultf_score')], by = 'key') %>%
    left_join(Cocreation[,c('key', 'cocr_score')], by = 'key') %>%
    left_join(Humanvalues[,c('key', 'human_score')], by = 'key') %>%
    left_join(Circular[,c('key', 'circ_score')], by = 'key') %>%
    left_join(Governance[,c('key', 'respg_score')], by = 'key') %>%
    mutate(caet_tot = mean(c(div_score,syn_score,eff_score,rec_score,res_score,cultf_score,cocr_score,human_score,circ_score,respg_score), na.rm = TRUE)) # nolint
  
  return(CAET)
}

data <- caet_CAET()


# write the data back into a Google Sheets for further analysis and visualization
gs4_auth()
sheet_write(data, 
            ss = "https://docs.google.com/spreadsheets/d/14Lo4lo6xupYjWTcAQywYKzlowT1p8yXijyUrqSpbh_0/edit?gid=440395557#gid=440395557", 
            sheet = "Sheet6")

taskscheduler_create(
  taskname = "UpdateKoboGoogleSheet",
  rscript = "C:/Users/Nkoro/OneDrive - Food and Agriculture Organization/TAPE/update_google_sheet.R",
  schedule = "ONCE",
  starttime = "23:17",
  Rexe = file.path(R.home("bin"), "Rscript.exe")
)
