library(data.table)
library(httr)
library(dplyr)
library(sf)
library(readr)
library(jsonlite)

get_land_serial_no_geometry <- function(land_serial_no, land_version = '111Oct'){
  request_headers <- c("user-agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
                       "cookie" = 'AGS_ROLES="419jqfa+uOZgYod4xPOQ8Q=="; _ga=GA1.1.580316533.1652926102',
                       "referer" = "https://map.coa.gov.tw/",
                       "origin" = "https://map.coa.gov.tw/")
  
  coagis_post <- POST("https://coagis.colife.org.tw/arcgis/rest/services/CadastralMap/SOE/MapServer/exts/CoaRESTSOE/LandAddressToLocation_ring",
                      timeout(30),
                      body = list("token" =  "sRwtJZ5mdzMWMzlReMnaRetlkCEs93-L0vZovzxHBET7zufCyMlwv251qN3_gYVO_NRFS7W8mZkaffdphSHpoQ..",
                                  "LandAddress" = land_serial_no,
                                  "LandVersion" = land_version,
                                  "CodeVersion" = land_version,
                                  "SpatialRefZone" = "本島",
                                  "SpatialRefOutput" = "4326",
                                  "f"= "json"),
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

land_serial_no_ok <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_OK.rds")
land_serial_no_lookup <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo.rds")
land_serial_no_lookup <- land_serial_no_lookup %>%
  filter(!land_serial_no %in% land_serial_no_ok$land_serial_no) %>%
  filter(!grepl("NA", land_serial_no)) %>%
  filter(!county_name %in% c("澎湖縣", "金門縣", '連江縣'))

s_list <- list()
i = 1
while (i < nrow(land_serial_no_lookup)){
  tryCatch({
    print(land_serial_no_lookup$land_serial_no[i])
    s <- get_land_serial_no_geometry(land_serial_no = land_serial_no_lookup$land_serial_no[i]) %>%
      cbind(dplyr::select(land_serial_no_lookup[i, ], -c("land_serial_no")), .) %>%
      list()
    s_list <- append(s_list, s)
    i = i + 1
    Sys.sleep(10.01)
  }, error = function(e){
    print(e)
    print("發生 timedout!")
    new_land_serial_no_ok_df <- s_list %>% bind_rows() %>% st_as_sf() %>% st_make_valid()
    land_serial_no_ok <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_OK.rds")
    land_serial_no_ok <- bind_rows(land_serial_no_ok, new_land_serial_no_ok_df) %>% distinct() %>% st_as_sf()
    land_serial_no_ok %>% saveRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_OK.rds")
    print("爬得資料已經暫存成功，list 即將清空 ...")
    land_serial_no_ok <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo_OK.rds")
    land_serial_no_lookup <- readRDS("C:/Users/User/Documents/taft_data/land_data/poolLandInfo.rds")
    land_serial_no_lookup <- land_serial_no_lookup %>%
      filter(!land_serial_no %in% land_serial_no_ok$land_serial_no) %>%
      filter(!grepl("NA", land_serial_no)) %>%
      filter(!county_name %in% c("澎湖縣", "金門縣", '連江縣'))
    s_list <- c()
    i = 1
    print("程式將在休息 30 秒之後繼續運作")
    Sys.sleep(30)
  }
  )
}
