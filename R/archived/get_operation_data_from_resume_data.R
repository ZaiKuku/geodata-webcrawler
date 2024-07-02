library(httr)
library(dplyr)
library(data.table)
library(tidyr)
library(magrittr)

rdss <- list.files("C:/Users/User/Documents/taft_data/resume_data/", pattern = ".rds$", full.names = T) %>%
  tibble() %>%
  rename(filepath = 1) %>%
  rowwise() %>%
  mutate(key = gsub(".rds$", "", tail(unlist(strsplit(filepath, "_")), 1)))

processed_rdss <- list.files("C:/Users/User/Documents/taft_data/operation_detail/", pattern = ".rds$", full.names = T) %>%
  tibble() %>%
  rename(filepath = 1) %>%
  rowwise() %>%
  mutate(key = gsub(".rds$", "", tail(unlist(strsplit(filepath, "_")), 1)))

rdss <- rdss %>%
  filter(!key %in% processed_rdss$key) %>%
  ungroup %>%
  arrange(filepath)

rds_list <- list()
# rds_list %>% length()
# rds_i = rdss[1]
for (rds_i in rdss$filepath){
  print(rds_i)
  rds_df <- rds_i %>%
    readRDS() %>%
    filter(ParentTraceCode == "") %>%
    filter(!grepl("號$", Place))
  
  operation_list <- list()
  i = 0
  # tracecode_i <- api_response_result_df$Tracecode[3]
  for (tracecode_i in rds_df$Tracecode){
    operation_list_tmp <- GET(paste('https://data.coa.gov.tw/Service/OpenData/Resume/OperationDetail_Plus.aspx?Tracecode=', tracecode_i, sep = "")) %>%
      content() %>%
      tibble() %>%
      unnest_wider(".") %>%
      mutate(Tracecode = tracecode_i) %>%
      list()
    
    operation_list <- append(operation_list, operation_list_tmp)
    
    if (i %% 100 == 0){
      print(i / length(rds_df$Tracecode) * 100)
    }
    i <- i + 1
  }
  
  operation_list %>%
    bind_rows() %>%
    saveRDS(gsub("resume_data", "operation_detail", rds_i))
  
}
