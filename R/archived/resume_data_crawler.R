library(httr)
library(dplyr)
library(data.table)
library(tidyr)
library(magrittr)
skip_i = 30000
for (skip_i in seq(3230000, 6000000, 10000)){
  print(paste("skip_i = ", skip_i, sep = ""))
  api_response <- GET(paste("https://data.coa.gov.tw/Service/OpenData/Resume/ResumeData_Plus.aspx?$top=10000&$skip=", as.integer(skip_i), sep = ""))
  api_response_result <- api_response %>%
    content
  api_response_result_df <- api_response_result %>%
    data.table %>%
    unnest_wider(".") %>%
    dplyr::relocate(Tracecode)
  
  api_response_result_df %>% saveRDS(paste("C:/Users/User/Documents/taft_data/resume_data/resume_data_", as.integer(skip_i), ".rds", sep = ""))
  
  # operation_list <- list()
  # i = 0
  # # tracecode_i <- api_response_result_df$Tracecode[3]
  # for (tracecode_i in api_response_result_df$Tracecode){
  #   operation_list_tmp <- GET(paste('https://data.coa.gov.tw/Service/OpenData/Resume/OperationDetail_Plus.aspx?Tracecode=', tracecode_i, sep = "")) %>%
  #     content() %>%
  #     tibble() %>%
  #     unnest_wider(".") %>%
  #     mutate(Tracecode = tracecode_i) %>%
  #     list()
  #   
  #   operation_list <- append(operation_list, operation_list_tmp)
  #   
  #   if (i %% 100 == 0){
  #     print(i / length(api_response_result_df$Tracecode) * 100)
  #   }
  #   i <- i + 1
  # }
  # 
  # operation_list %>%
  #   bind_rows() %>%
  #   saveRDS(paste("C:/Users/User/Documents/taft_data/operation_detail/operation_detail_", skip_i, ".rds", sep = ""))
  
}
