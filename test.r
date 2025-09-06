library(tidyverse)
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
data <- read_delim(csv_content, delim = ";")
write_csv(data, "kobo_exports/form_test_clone.csv")


view(data)




res <- GET(
  url = "https://eu.kobotoolbox.org/api/v2/assets/",
  authenticate(username, password)
)
assets_json <- content(res, as = "text", encoding = "UTF-8")
assets_list <- fromJSON(assets_json)



# Extract the list of forms (assets)
forms_df$asset_id <- sub(".*/assets/([a-z0-9-]+)/$", "\\1", forms_df$url)
head(forms_df[, c("name", "uid", "asset_id")])

for (i in seq_len(nrow(forms_df))) {
  asset_id <- forms_df$asset_id[i]
  export_url <- paste0("https://eu.kobotoolbox.org/api/v2/assets/", asset_id, "/data.csv") # nolint
  
  res <- GET(export_url, authenticate(username, password))
  
  if (status_code(res) == 200) {
    csv_content <- content(res, as = "text", encoding = "UTF-8")
    data <- read_csv(csv_content)
    cat("✅ Downloaded data for:", forms_df$name[i], "\n")
  } else {
    cat("❌ Failed for:", forms_df$name[i], " (status:", status_code(res), ")\n")
  }
}

forms_df[, c("name", "has_deployment", "deployment__submission_count")]


forms <- assets_list$results
forms_df <- as.data.frame(forms)
names(forms_df)
print(forms_df[, c("name", "uid")]) # Show form names and IDs

# Loop through each asset and download its data as CSV
for (i in seq_len(nrow(forms_df))) {
  asset_id <- forms_df$uid[i]
  export_url <- paste0("https://eu.kobotoolbox.org/api/v2/assets/", asset_id, "/data.csv") # nolint
  res <- GET(export_url, authenticate(username, password))
  if (status_code(res) == 200) {
    csv_content <- content(res, as = "text", encoding = "UTF-8")
    # Save or process csv_content as needed, e.g.:
    data <- read_csv(csv_content) # nolint
    cat("Downloaded data for asset:", asset_id, "\n")
  } else {
    cat("Failed to download asset:", asset_id, "\n")
  }
}

data <- read_delim(csv_content, delim = ";")
data <- data %>%
  select(-c(`_uuid`, `_submission_time`, `_tags`, `_notes`, `_status`, `_edited`, `_xform_id_string`, `_attachments`, `_geolocation`, `_geoshape`, `_geotrace`, `start`, `end`, `deviceid`, `subscriberid`, `simserial`, `phonenumber`)) # nolint: line_length_linter.

#### Diversity
##The Diversity score is calculated by the sum of the amount 
of **cultivated crops**[score 0-4], amount of **tree species** [score 0-4], amount of **animal species** [score 0-4] and amount of **income producing activities** (div_activ) score [0-4]. this sum is then divided by 16 and multiplied by 100.
caet_diversity <- function(){
  Diversity <- data[,c('key', 'crops', 'animals', 'trees', 'div_activ')]
  Diversity <- Diversity %>%
    rowwise() %>%  #cleans data so there only is one number for crops animals trees ...
    mutate(div_sum = sum(c(crops, animals, trees, div_activ), na.rm = TRUE),
           div_aver = mean(c(crops ,animals ,trees ,div_activ),na.rm = TRUE),  # Is later used for Resilience # nolint
           div_score = round((div_sum/16)*100,6)) # calculates score
  return(Diversity)
  }

### Synergies
##The Synergy score is calculated by the sum of the **crop-animal-integration** (cla_int) score [0-4], the **soil-plant-management** score [0-4], the **integration of trees** [score 0-4] and a score for the **connectivity of the farm to the landscape** [0-4]. This sum is then divided by 16 and multiplied by 100.
caet_synergies <- function(){
  Synergies <- data[,c('key', 'cla_int', 's_plant', 'tree_int', 'connectivity')]
  Synergies <- Synergies %>% #creates dataframe to calculate indicator
    rowwise() %>%
    mutate(syn_sum = sum(c(cla_int ,s_plant ,tree_int ,connectivity),na.rm = TRUE),
           syn_score = round((syn_sum/16)*100,6))
  return(Synergies)
}


### Efficiency
##The Efficiency score is calculated by the sum of the **use of external inputs** [score 0-4], **Management of soil fertility** [score 0-4], **Management of pests and diseases** [score 0-4] and by a score of e**merging efficiency from good practices** [score 0-4]. This sum is the divided by 16 and multiplied by 100.
caet_efficiency <- function(){
  Efficiency <- data[,c('key', 'ext_inp', 'soil_fert', 'pest_dis', 'emergingefficiency')]
  Efficiency <- Efficiency %>%
    rowwise() %>%
    mutate(eff_sum = sum(c(ext_inp, soil_fert, pest_dis, emergingefficiency), na.rm = TRUE),
           eff_score = round((eff_sum/16)*100,6))
  return(Efficiency)
}

### Recycling
##The Recycling score is calculated by the sum of **recycled biomass** [score 0-4], the **waste production and management** score [0-4], the **water recycling & saving** score [0-4], and **energy reduction and** **renewable energy** score [0-4]. This sum is then divided by 16 and multiplied by 100.
caet_recycling <- function(){
  Recycling <- data[,c('key', 'rec_biomass', 'waste', 'water', 'ren_energy')]
  Recycling <- Recycling %>%
    rowwise() %>%
    mutate(rec_sum = sum(c(rec_biomass, waste, water, ren_energy),na.rm = TRUE),
           rec_score = round((rec_sum/16)*100,6))
  return(Recycling)
}

### Resilience
##The Resilience score is calculated by the sum of **Existence of social mechanisms to reduce vulnerability** (vuln) [score 0-4], the **Environmental resilience and capacity to adapt to climate change** (indebt) [score 0-4], the mean of the 4 diversity (div_aver) scores, and the average score of the **indices measuring self-sufficiency and empowerment** (aver_suss_emp). The sum is then divided by 16 and multiplied by 100.
caet_resilience <- function(){
  Resilience <- data[,c('key', 'crops', 'animals', 'trees', 'div_activ', 'ext_inp', 'seeds_breeds', 'local_fs', 'prod_empow', 'vuln', 'indebt')]
  Resilience <- Resilience %>%
    rowwise() %>%
    mutate(div_aver = mean(c(crops, animals, trees, div_activ), na.rm = TRUE), # emerging resilience from diversity
           aver_suff_emp = mean(c(ext_inp, seeds_breeds, local_fs, prod_empow), na.rm = T), # average score of the indices measuring self-sufficiency and empowerment
           res_sum = sum(c(vuln, indebt, div_aver, aver_suff_emp), na.rm = TRUE),
           res_score = round((res_sum/16)*100,6))
  return(Resilience)
}

### Culture and food traditions
##The Culture and food traditions score is calculated by the sum of a score for **nutritional diet** (diet) [score 0-4], the score for **food self-sufficiency** (food_self_suff) [score 0-4], the **Local and traditional food heritage** (food_heritage) [score 0-4] and the **management of seeds and breeds** (seeds_breeds) [score 0-4]. The sum is then divided by 16 and multiplied by 100.
caet_culture <- function(){
  Culture <- data[,c('key', 'diet', 'food_self_suff', 'food_heritage', 'seeds_breeds')]
  Culture <- Culture %>%
    rowwise() %>%
    mutate(cultf_sum = sum(c(diet, food_self_suff, food_heritage, seeds_breeds),na.rm = TRUE),
           cultf_score = round((cultf_sum/16)*100,6))
  return(Culture)
}

### Co-creation
##The Co-creation score is calculated by the sum of the **availability of social platforms for knowledge transfer** (platforms)[score 0-4], **access and interest to agroecological knowledge** (ae_know) [score 0-4] and the **interconnection of producers to their community and grassroot networks** (partic_orgs) [score 0-4]. This sum is then divided by 12 and multiplied by 100.
caet_cocreation <- function(){
  Cocreation <- data[,c('key', 'platforms', 'ae_know', 'partic_orgs')]
  Cocreation <- Cocreation %>%
    rowwise() %>%
    mutate(cocr_sum = sum(c(platforms, ae_know, partic_orgs),na.rm = TRUE),
           cocr_score = round((cocr_sum/12)*100,6))
  return(Cocreation)
}

### Human and social values
##The score for human and social values is calculated by the sum of the **women empowerment** (women) score [0-4], a score that measures **working conditions, wages and family participation** (labour) [score 0-4], a score of weather the **youth wants to continue farming** (youth) [0-4] and a **animal welfare** (animalwel) score [0-4]. Then the sum is divided by 16 (or 12 if there are no animals on the farm) and multiplied by 100.
caet_humanvalues <- function(){
  Humanvalues <- data[,c('key', 'women', 'labour', 'youth', 'animalwel')]
  Humanvalues <- Humanvalues %>%
    rowwise() %>%
    mutate(human_sum = sum(c(women, labour, youth, animalwel),na.rm = TRUE),
           human_score = round((human_sum/16)*100,6),
           human_score = ifelse(is.na(animalwel),round((human_sum/12)*100,6),human_score))
  return(Humanvalues)
}

### Circular economy and solidarity
##The score for circular economy and solidarity is calculated by the sum of a score for **local marketing** (mkt_local) [score 0-4], **relationship with the consumers** (networks) [score 0-4] and and a score for the **local food system** (local_fs) [score 0-4]. The sum is then divided by 12 and multiplied by 100.
caet_circular <- function(){
  Circular <- data[,c('key', 'mkt_local', 'networks', 'local_fs')]
  Circular <- Circular %>%
    rowwise() %>%
    mutate(circ_sum = sum(c(mkt_local, networks, local_fs),na.rm = TRUE),
           circ_score = round((circ_sum/12)*100,6))
  return(Circular)
}

### Responsible Governance
##The score for responsible governance is calculated by the sum of the **producers rights and bargaining power** [score 0-4], **the cooperation between producers** [score 0-4] and the **possibility to participate in land governance** [score 0-4]. The sum is then divided by 12 and multiplied by 100.
caet_governance <- function(){
  Governance <- data[,c('key', 'prod_empow', 'prod_orgs', 'partic_prod')]
  Governance <- Governance %>%
    rowwise() %>%
    mutate(respg_sum = sum(c(prod_empow, prod_orgs, partic_prod),na.rm = TRUE),
           respg_score = round((respg_sum/12)*100,6))
  return(Governance)
}

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
    mutate(caet_tot = mean(c(div_score,syn_score,eff_score,rec_score,res_score,cultf_score,cocr_score,human_score,circ_score,respg_score), na.rm = TRUE))
  
  return(CAET)
}
data <- caet_CAET()
#Google Sheets
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
