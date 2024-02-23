library(data.table)
library(httr)
library(dplyr)
library(sf)
library(readr)
library(jsonlite)
library(DBI)
library(magrittr)
library(geojsonsf)

conn_taft <- DBI::dbConnect(RMariaDB::MariaDB(),
                            username = "datayoo",
                            password = "*@(!)@&#",
                            dbname = "taft",
                            port = "3306",
                            host = "192.168.1.103")

conn_land <- DBI::dbConnect(RMariaDB::MariaDB(),
                            username = "datayoo",
                            password = "*@(!)@&#",
                            dbname = "land",
                            port = "3306",
                            host = "192.168.1.103")

section_info_raw <- dbGetQuery(conn_land, "select county_name, town_name, unit_id, section_id, section_name 
                               from section_info")

DBI::dbDisconnect(conn_land)

unknown_lands <- dbGetQuery(conn_taft, "select distinct resume_land_info.unit_id, resume_land_info.section_id, resume_land_info.full_land_no 
           from resume_land_info left join land_info on
           resume_land_info.unit_id = land_info.unit_id and
           resume_land_info.section_id = land_info.section_id and
           resume_land_info.full_land_no = land_info.full_land_no
           where land_info.land_id is null")

get_land_serial_no_geometry <- function(land_serial_no, land_version = '112Oct'){
  request_headers <- c("Host" = "coagis.colife.org.tw",
                       "Origin" = "https://map.moa.gov.tw",
                       "Referer" = "https://map.moa.gov.tw/")
  
  coa_token <- 'sRwtJZ5mdzMWMzlReMnaRdRQabgGgnHu4zDtSY7JkVbgMRprP-dkv-84cczetfl4YLck-rQmvCIlxiyYoFdxIg..'
  land_serial_no_encoded <- URLencode(land_serial_no)
  
  request_url <- paste('https://coagis.colife.org.tw/arcgis/rest/services/CadastralMap/SOE/MapServer/exts/CoaRESTSOE/LandAddressToLocation_ring?',
                       'token=', coa_token, "&",
                       'LandAddress=', land_serial_no_encoded, '&',
                       'LandVersion=', land_version, '&',
                       'CodeVersion=', land_version, '&',
                       'SpatialRefZone=', URLencode("本島"), '&',
                       'SpatialRefOutput=', '4326', '&',
                       'f=json',
                       sep = "")
  
  coagis_post <- GET(request_url,
                     timeout(10),
                     add_headers(request_headers)
  )
  
  coagis_post_result <- coagis_post %>% content
  
  coagis_post_result_sf <- coagis_post_result$ReturnResult[[1]]$ReturnPolygon %>% gsub('rings"','type\":\"MultiPolygon\",\"coordinates\"',.) %>% 
    gsub("\\[\\[\\[","\\[\\[\\[\\[",.) %>%
    gsub("\\]\\]\\]","\\]\\]\\]\\]",.) %>%
    paste("[",.,"]",sep="")  %>%
    geojsonsf::geojson_sfc() %>%
    st_as_sf(crs = 4326) %>%
    mutate(land_serial_no = land_serial_no,
           return_land_serial_no = coagis_post_result$ReturnResult[[1]]$ReturnLandAddress) %>%
    rename(geometry = x) %>%
    return()
}

get_s_n_id_geometry <- function(unit_id, section_id, full_land_no){
  request_headers <- c("Host" = "taft.moa.gov.tw",
                       "Origin" = "https://taft.moa.gov.tw")
  
  api_response <- POST('https://taft.moa.gov.tw/sp-resume-service-1.html',
                       body = list("action" = "Geo_OP",
                                   "SID" = paste(unit_id, section_id, sep = ''),
                                   "NID" = full_land_no),
                       add_headers(request_headers))
  
  api_response_content <- api_response %>%
    content()
  
  api_response_result <- api_response_content %>%
    fromJSON() %$%
    success
  
  if (api_response_result == FALSE){
    print("taft land api returns nothing!")
    return(tibble())
  }
  
  api_response_ring <- api_response_content %>%
    fromJSON() %$%
    data %>%
    dplyr::select(geometry) %>%
    toJSON(auto_unbox = T, digits = 8) %>%
    as.character() %>%
    substr(., 23, nchar(.) - 3) 
  
  if (substr(api_response_ring, 1, 4) != "[[[["){
    api_response_ring <- paste("[", api_response_ring, "]", sep = '')
  }
  
  api_geometry <- api_response_ring %>%
    paste('[{"type":"MultiPolygon","coordinates":', ., "}]", sep = "") %>%
    geojson_sf() %>%
    st_make_valid() 
  
  return(api_geometry)
}

st_geometry_coordinates_text <- function(geom){
  library(geojsonsf)
  library(magrittr)
  library(jsonlite)
  geom_char_string <- c()
  geom <- geom %>% sfc_geojson
  for (i in c(1:length(geom))){
    if ((geom[i])=="null"){
      geom_i_char_string <- NA_character_
    } else {
      type_for_check <-(geom[i]) %>% fromJSON %$% type
      if (type_for_check == "GeometryCollection"){
        geom_i_char_string <- (geom[i]) %>% fromJSON %$% geometries %$% coordinates %>% toJSON(digits = 8) %>% as.character %>% try(silent = T)
      } else {
        geom_i_char_string <- (geom[i]) %>% fromJSON %$% coordinates %>% toJSON(digits = 8) %>% as.character %>% try(silent = T)
      }
    }
    geom_char_string <- c(geom_char_string,geom_i_char_string)
  }
  return(geom_char_string)
}

unknown_lands_processed <- unknown_lands %>%
  left_join(., section_info_raw, by = c("unit_id", "section_id")) %>%
  mutate(town_name = case_when(is.na(town_name) ~ county_name,
                               TRUE ~ town_name)) %>%
  mutate(parcel_1 = as.numeric(substr(full_land_no, 1, 4)),
         parcel_2 = as.numeric(substr(full_land_no, 5, 8))) %>%
  mutate(land_serial_no = paste(county_name, town_name, section_name, parcel_1, "-", parcel_2, "地號", sep = "")) %>%
  mutate(land_serial_no = gsub("-0", "", land_serial_no)) %>%
  filter(!grepl("NA", land_serial_no)) %>%
  filter(!county_name %in% c("澎湖縣", "金門縣", '連江縣'))

res_list <- list()

need_newold_conversion_rds <- '/mnt/pic_satellite/taft_data/land_data/need_newold_conversion.rds'
if (!file.exists(need_newold_conversion_rds)){
  need_newold_conversion_rds_list <- list()
} else {
  need_newold_conversion_rdss <- need_newold_conversion_rds %>%
    readRDS()
  unknown_lands_processed <- unknown_lands_processed %>%
    anti_join(., need_newold_conversion_rdss, by = c("unit_id", "section_id", "full_land_no"))
  need_newold_conversion_rds_list <- need_newold_conversion_rdss %>%
    list()
}

for (i in c(nrow(unknown_lands_processed):1)){
  print(unknown_lands_processed$land_serial_no[i])
  print(unknown_lands_processed$unit_id[i])
  print(unknown_lands_processed$section_id[i])
  print(unknown_lands_processed$full_land_no[i])
  
  Sys.sleep(2 + runif(1, 1, 5))
  
  taft_result_i <- get_s_n_id_geometry(unit_id = unknown_lands_processed$unit_id[i],
                                       section_id = unknown_lands_processed$section_id[i],
                                       full_land_no = unknown_lands_processed$full_land_no[i])
  
  if (nrow(taft_result_i) == 1){
    res_list_tmp <- cbind(taft_result_i, unknown_lands_processed[i, ]) %>%
      st_as_sf() %>%
      list()
    
    res_list_tmp_check <- res_list_tmp %>%
      bind_rows() %>%
      st_as_sf() 
  } else {
    print("now tries to ask moa land geometry api ...")
    
    result_i <- try(get_land_serial_no_geometry(land_serial_no = unknown_lands_processed$land_serial_no[i], '112Oct'), silent = T)
    
    if (!is.null(attr(result_i, "class"))){
      if (attr(result_i, "class")[1] == "try-error"){
        print("timeout! now is going to skip this land ...")
        next
      }
    }
    
    res_list_tmp <- inner_join(unknown_lands_processed[i, ], result_i, by = "land_serial_no") %>%
      st_as_sf %>%
      list()
    
    res_list_tmp_check <- res_list_tmp %>%
      bind_rows() %>%
      st_as_sf() %>%
      dplyr::filter(land_serial_no == return_land_serial_no)
  }
  
  if (nrow(res_list_tmp_check) == 0){
    print("different input and output land_serial_no of API!")
    need_newold_conversion_rds_list_i <- (unknown_lands_processed[i, ]) %>%
      dplyr::select(unit_id, section_id, full_land_no) %>%
      list()
    need_newold_conversion_rds_list <- append(need_newold_conversion_rds_list, need_newold_conversion_rds_list_i)
    need_newold_conversion_rds_list %>%
      rbindlist %>%
      saveRDS(need_newold_conversion_rds)
    next
  }
  
  land_info_tbw <- res_list_tmp_check %>%
    st_make_valid() %>%
    mutate(coordinates = st_geometry_coordinates_text(geometry),
           geometry_type = as.character(st_geometry_type(geometry))) %>%
    mutate(geometry_type = case_when(geometry_type == 'POLYGON' ~ "Polygon",
                                     geometry_type == 'MULTIPOLYGON' ~ "MultiPolygon")) %>%
    st_drop_geometry() %>%
    dplyr::select(unit_id, section_id, full_land_no, land_serial_no, coordinates, geometry_type)
  
  dbWriteTable(conn_taft, "land_info", land_info_tbw, append = T)
  
}