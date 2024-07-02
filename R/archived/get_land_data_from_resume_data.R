library(DBI)
library(httr)
library(dplyr)
library(data.table)
library(tidyr)
library(magrittr)
library(stringr)

conn_social <- DBI::dbConnect(RMariaDB::MariaDB(),
                           username = "datayoo",
                           password = "*@(!)@&#",
                           dbname = "social",
                           port = "3306",
                           host = "192.168.1.103")

rdss <- list.files("C:/Users/User/Documents/taft_data/resume_data/", pattern = ".rds$", full.names = T) %>%
  tibble() %>%
  rename(filepath = 1) %>%
  rowwise() %>%
  mutate(key = gsub(".rds$", "", tail(unlist(strsplit(filepath, "_")), 1)))

land_processed_rdss <- list.files("C:/Users/User/Documents/taft_data/land_data/", pattern = ".rds$", full.names = T) %>%
  tibble() %>%
  rename(filepath = 1) %>%
  rowwise() %>%
  mutate(key = gsub(".rds$", "", tail(unlist(strsplit(filepath, "_")), 1)))

rdss <- rdss %>%
  filter(!key %in% land_processed_rdss$key)

section_info_raw <- dbGetQuery(conn_social, "select county_name, town_name, unit_id, section_id, section_name from section_info")

for (rds_i in rdss$filepath){
  rds_df <- rds_i %>%
    readRDS() %>%
    filter(ParentTraceCode == "") %>%
    filter(!grepl("號$", Place))

  rds_land_info_1 <- rds_df$LandSecNO %>%
    strsplit("\\;") %>%
    unlist() %>%
    str_subset("\\,") %>%
    unique() %>%
    str_split("\\,") %>%
    tibble %>%
    unnest_wider(".", names_sep = "_") 
  
  if (nrow(rds_land_info_1) == 0){
    next
  }
  
  rds_land_info_2 <- rds_land_info_1 %>%
    mutate(unit_id = substr(`._1`, 1, 2),
           section_id = substr(`._1`, 3, 6),
           land_no = paste(as.integer(substr(._2, 1, 4)), as.integer(substr(._2, 5, 9)), sep = "-")) %>%
    mutate(land_no = gsub("-0", "", land_no)) %>%
    left_join(., section_info_raw, by = c("unit_id", "section_id")) %>%
    mutate(town_name = case_when(is.na(town_name) ~ county_name,
                                 TRUE ~ town_name)) %>%
    mutate(land_serial_no = paste(county_name, town_name, section_name, land_no, "地號", sep = "")) %>%
    mutate(LandSecNO = paste(`._1`, `._2`, sep = ";")) %>%
    rename(unit_section_id = 1, land_no_2 = 2)

  rds_land_info_2 %>% saveRDS(gsub("resume_data", "land_data", rds_i))
}


