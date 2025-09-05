library(tidyverse)
library(httr)
library(readr)
library(googlesheets4)
library(taskscheduleR)

# Kobo API call with token
res <- GET("https://eu.kobotoolbox.org/api/v2/assets/atAgVczcWWXH2R6YsoWnXy/export-settings/esV9hRQi8jHt3qvooMrcB4N/data.csv",
           add_headers(Authorization = "5e8b1c356365fd9379cac934d1b7446396cdae95"))

csv_content <- content(res, as = "text", encoding = "UTF-8")

data <- read_delim(csv_content, delim = ";")
data <- data %>%
  select(-c(`_uuid`, `_submission_time`, `_tags`, `_notes`, `_status`, `_edited`, `_xform_id_string`, `_attachments`, `_geolocation`, `_geoshape`, `_geotrace`, `start`, `end`, `deviceid`, `subscriberid`, `simserial`, `phonenumber`))

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
  