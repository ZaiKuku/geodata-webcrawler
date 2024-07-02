library(sf)
library(httr)
library(dplyr)

taft_lands <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_w_pdu_name_id.rds")
taft_pdu_lists <- taft_lands %>%
  dplyr::select(taft_land_id, product) %>%
  st_drop_geometry()

taft_pdu_lists %>%
  tibble


while (1){
  print(Sys.time())
  taft_lands_w_satellite <- readRDS("C:/Users/User/Documents/taft_data/land_satellite_data/land_satellite_data.rds")
  
  taft_land_i <- taft_lands[setdiff(taft_lands$taft_land_id, taft_lands_w_satellite$taft_land_id), ] %>%
    sample_n(1)
  taft_land_i_land_id <- taft_land_i$taft_land_id
  
  headers = c(
    'Apikey' = 'z3tw3rm6e5DxA2aY'
  )
  file.remove('C:/Users/User/Documents/env_api_pushed/API_example/taft_tmp.geojson')
  taft_land_i %>% st_write('C:/Users/User/Documents/env_api_pushed/API_example/taft_tmp.geojson', overwrite = T)
  body = list(
    'index_name' = 'NDRE',
    'start_time' = '2019-01-01',
    'end_time' = '2023-05-31',
    'time_interval' = 'day',
    'grid_size' = '10',
    'field_info' = upload_file('C:/Users/User/Documents/env_api_pushed/API_example/taft_tmp.geojson')
  )
  
  res <- VERB("POST", url = "http://192.168.1.103:8888/api/satellite/32TemporalVegeIndexInGrids", body = body, add_headers(headers), encode = 'multipart')
  file.remove('C:/Users/User/Documents/env_api_pushed/API_example/taft_tmp.geojson')
  
  res %>%
    content %$%
    result %>%
    data.table() %>%
    unnest_wider(".")  %>%
    mutate(taft_land_id = taft_land_i_land_id) %>%
    rbind(., taft_lands_w_satellite) %>%
    saveRDS("C:/Users/User/Documents/taft_data/land_satellite_data/land_satellite_data.rds")
}

taft_lands_w_satellite$taft_land_id %>%
  unique %>%
  sort %>%
  head

taft_lands_w_satellite %>%
  filter(taft_land_id == 9) %>%
  View

taft_lands_w_satellite %>%
  filter(taft_land_id == 9) %>%
  filter(!is.na(index)) %>%
  mutate(time = as_date(time)) %$%
  plot(time, index, type = "l")
taft_pdu_lists_wo_jp <- taft_pdu_lists %>%
  filter(!grepl("バナナ", product))

taft_lands_w_satellite_wo_jp <- taft_lands_w_satellite %>%
  filter(taft_land_id %in% taft_pdu_lists_wo_jp$taft_land_id)

taft_lands_w_satellite_wo_jp_unique_id <- taft_lands_w_satellite_wo_jp$taft_land_id %>% unique

for (i in c(1:length(taft_lands_w_satellite_wo_jp_unique_id))){
  print(i)
  sample_land_id <- taft_lands_w_satellite_wo_jp_unique_id[i]
  product_chn_name <- taft_pdu_lists_wo_jp %>%
    filter(taft_land_id == sample_land_id) %$%
    product
  dir.create(paste("C:/Users/User/Documents/taft_data/land_satellite_data/plotting/", product_chn_name, sep = ""))
  png(filename = paste("C:/Users/User/Documents/taft_data/land_satellite_data/plotting/", product_chn_name, "/", "plot_", sample_land_id, ".png", sep = ""))
  taft_lands_w_satellite %>%
    dplyr::filter(taft_land_id == sample_land_id) %>%
    mutate(index = zoo::na.approx(index, na.rm = F)) %>%
    mutate(time = as_date(time)) %>%
    summarise(index_ma = zoo::rollmean(index, 18),
              time = zoo::rollmean(time, 18),
              taft_land_id = taft_land_id[1]) %>%
    left_join(., taft_pdu_lists_wo_jp, by = "taft_land_id") %>%
    filter(!is.na(index_ma)) %$%
    #filter(cloud_cover == 0) %>%
    plot(time, index_ma, type = "l", ylim = c(0, 1), main = product[1]) 
  dev.off()
}

ticc <- Sys.time()
taft_lands_w_satellite %>%
  data.table() %>%
  .[taft_land_id == sample_land_id, ] 
Sys.time() - ticc

ticc <- Sys.time()
taft_lands_w_satellite %>%
  dplyr::filter(taft_land_id == sample_land_id) %>%
  invisible()
Sys.time() - ticc