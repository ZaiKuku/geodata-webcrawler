library(DBI)
library(httr)
library(dplyr)
library(tidyr)
library(magrittr)
library(stringr)
library(data.table)

for (skip_i in seq(0, 1000000, 10000)){
  print(paste("skip_i = ", skip_i, sep = ""))
  
  api_response <- GET(paste("https://data.coa.gov.tw/Service/OpenData/Resume/ResumeData_Plus.aspx?$top=10000&$skip=", as.integer(skip_i), sep = ""))
  
  api_response_result <- api_response %>%
    content
  
  api_response_result_df <- api_response_result %>%
    data.table %>%
    unnest_wider(".") %>%
    dplyr::relocate(Tracecode)
  
  api_response_result_df %>%
    saveRDS(paste("/mnt/pic_satellite/taft_data/resume_data/d20240215/resume_data_", as.integer(skip_i), ".rds", sep = ""))
  
}

conn_taft <- DBI::dbConnect(RMariaDB::MariaDB(),
                            username = "datayoo",
                            password = "*@(!)@&#",
                            dbname = "taft",
                            port = "3306",
                            host = "192.168.1.103")

rdss <- list.files("/mnt/pic_satellite/taft_data/resume_data/d20240215/", pattern = ".rds$", full.names = T) %>%
  tibble() %>%
  rename(filepath = 1) %>%
  rowwise() %>%
  mutate(key = gsub(".rds$", "", tail(unlist(strsplit(filepath, "_")), 1)))

rdss_i = rdss$filepath[1]

rdss_list <- list()

for (rdss_i in rdss$filepath){
  rdss_list_tmp <- readRDS(rdss_i) %>%
    list()
  
  rdss_list <- append(rdss_list, rdss_list_tmp)
}

agr_resume_data <- rdss_list %>%
  rbindlist() %>%
  distinct() 

agr_resume_data <- agr_resume_data %>%
  dplyr::filter(as.numeric(substr(Tracecode, 1, 1)) <= 1) %>%
  dplyr::filter(as.numeric(substr(OrgID, 1, 1)) <= 1) %>%
  dplyr::filter(ParentTraceCode == '')

resume_data_tbw <- agr_resume_data %>%
  rowwise() %>%
  mutate(sub_product_name = head(unlist(str_split(ProductName, "-")), 1)) %>%
  ungroup %>%
  dplyr::select(trace_code = Tracecode, # 追蹤碼
                product_name = ProductName, # 產品名稱
                org_id = OrgID, # 生產者組織代碼
                org_name = Producer, # 農業經營業者
                farmer_name = FarmerName,
                pkg_date = PackDate, # 包裝日期
                store_info = StoreInfo, # 通路商資訊
                sub_product_name
  )

resume_land_info_tbw <- agr_resume_data %>%
  dplyr::select(Tracecode, LandSecNO) %>%
  rowwise() %>%
  mutate(LandSecNO = strsplit(LandSecNO, ";")) %>%
  unnest_longer("LandSecNO") %>%
  mutate(unit_id = substr(LandSecNO, 1, 2),
         section_id = substr(LandSecNO, 3, 6),
         land_no_2 = substr(LandSecNO, 8, 15)) %>%
  dplyr::filter(nchar(land_no_2) == 8) %>%
  dplyr::select(-LandSecNO) %>%
  rename(trace_code = Tracecode, full_land_no = land_no_2) %>%
  distinct()

resume_data_existed_tc <- dbGetQuery(conn_taft, "select distinct(trace_code) as trace_code from resume_data")
resume_land_info_existed_tc <- dbGetQuery(conn_taft, "select distinct(trace_code) as trace_code from resume_land_info")

print('resume_data writing ... ')

resume_data_tbw %>%
  dplyr::filter(!trace_code %in% resume_data_existed_tc$trace_code) %>%
  DBI::dbWriteTable(conn_taft, "resume_data", ., append = T)

print('resume_land_info writing ... ')
resume_land_info_tbw %>%
  dplyr::filter(!trace_code %in% resume_data_existed_tc$trace_code) %>%
  DBI::dbWriteTable(conn_taft, "resume_land_info", ., append = T)