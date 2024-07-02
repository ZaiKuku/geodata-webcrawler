lsd <- readRDS("C:/Users/User/Documents/taft_data/land_satellite_data/land_satellite_data.rds")

readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_OK.rds")
readRDS("C:/Users/User/Documents/taft_data/resume_data/resume_data_2270000.rds") %>%
  View
readRDS("C:/Users/User/Documents/taft_data/operation_detail/operation_detail_2350000.rds") %>%
  View

rm(lsd)
gc()


operation_detail <- list.files("C:/Users/User/Documents/taft_data/operation_detail", pattern = ".rds", full.names = T)
operation_detail_list <- list()
operation_detail_i = operation_detail[1]
for (operation_detail_i in operation_detail){
  operation_detail_list_tmp <- readRDS(operation_detail_i) %>%
    dplyr::filter(Tracecode %in% resume_data_df_tracecode) %>%
    list()
  operation_detail_list <- append(operation_detail_list, operation_detail_list_tmp)
}

(operation_detail_list[[1]]) %>%
  head(100) %>%
  write.table("C:/Users/User/Documents/taft_data/agr_operation_detail_demo.csv", sep = ",", quote = T, row.names = F)

start_seq <- seq(1,391,50)
end_seq <- start_seq + 49
end_seq[length(end_seq)] <- 391

for (i in c(1:length(start_seq))){
operation_detail_df <- operation_detail_list[c(start_seq[i] : end_seq[i])] %>%
  rbindlist()
operation_detail_df %>%
  saveRDS(paste("C:/Users/User/Documents/taft_data/agr_operation_detail", i,".rds", sep = ""))
}

operation_detail_list %>%
  rbindlist() %>%
  saveRDS("C:/Users/User/Documents/taft_data/agr_operation_detail.rds")
gc()
# rm(operation_detail_list)
# gc()


resume_data <- list.files("C:/Users/User/Documents/taft_data/resume_data", pattern = ".rds", full.names = T)
resume_data_list <- list()

library(magrittr)
library(dplyr)
library(data.table)

resume_data_list_tmp <- resume_data %>%
  sample(., 1) %>%
  readRDS()

rs_detail <- resume_data_list_tmp %$%
  ProcessDetail %>%
  sample(., 1000)

rs_detail_i = rs_detail[200]
ct_df_list <- list()
library(httr)
library(tidyr)

for (rs_detail_i in rs_detail){
  
  ct <- GET(rs_detail_i) %>%
    content
  
  if (length(ct) != 0){
    ct_df <- ct %>%
      data.table() %>%
      unnest_wider(".") %>%
      data.table()
    
    # names(ct_df) <- unlist(ct_df[1, ])
    ct_df_list_tmp <- ct_df %>%
      list
    
    ct_df_list <- append(ct_df_list, ct_df_list_tmp)
    
    if (length(ct_df_list) == 20){
      stop("enough")
    }
  }
}

ct_df_list %>%
  rbindlist() %>%
  write.table("C:/Users/User/Documents/taft_data/agr_process_detail_demo.csv", sep = ",", quote = T, row.names = F)

ct_df_list %>%
  lapply(., function(x) data.table(t(x)))

ct_df_list %>%
  lapply(., function(x) data.table(t(x))) %>%
  rbindlist(fill = T) %>%
  write.table("C:/Users/User/Documents/taft_data/agr_certificate_detail_demo.csv", sep = ",", quote = T, row.names = F)
  rbindlist() %>%
  View


ct_df_list %>%
  rbindlist() %>%
  write.table("C:/Users/User/Documents/taft_data/agr_process_detail_demo.csv", sep = ",", quote = T, row.names = F)


resume_data_i = resume_data[1]
for (resume_data_i in resume_data){
  print(resume_data_i)
  resume_data_list_tmp <- readRDS(resume_data_i) %>%
    dplyr::filter(substr(OrgID, 1, 1) %in% c("1", "2")) %>%
    list()
  resume_data_list <- append(resume_data_list, resume_data_list_tmp)
}

resume_data_list_tmp %>%
  rbindlist() %>%
  sample_n(10) %>%
  write.table("C:/Users/User/Documents/taft_data/agr_resume_data_demo.csv", sep = ",", quote = T, row.names = F)


resume_data_df <- resume_data_list %>%
  rbindlist() %>%
  distinct(Tracecode, .keep_all = T) 

resume_data_df %>%
  saveRDS("C:/Users/User/Documents/taft_data/agr_resume_data.rds")

resume_data_df_tracecode <- resume_data_df$Tracecode

# rm(resume_data_df)
# gc()