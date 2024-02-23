library(DBI)
library(dplyr)
library(tidyr)
library(magrittr)
library(httr)

conn_taft <- DBI::dbConnect(RMariaDB::MariaDB(),
                            username = "datayoo",
                            password = "*@(!)@&#",
                            dbname = "taft",
                            port = "3306",
                            host = "192.168.1.103")

trace_code_raw <- dbGetQuery(conn_taft, "SELECT DISTINCT resume_data.trace_code
                                         FROM resume_data
                                         LEFT JOIN resume_operation_detail_info ON
                                         resume_data.trace_code = resume_operation_detail_info.trace_code
                                         WHERE operation_type IS NULL;")

for (tracecode_i in trace_code_raw$trace_code){
  print(tracecode_i)
  operation_list_tmp <- GET(
    paste("https://data.coa.gov.tw/Service/OpenData/Resume/OperationDetail_Plus.aspx?Tracecode=",
          tracecode_i,
          sep = "")
    ) %>%
    content() %>%
    tibble() %>%
    unnest_wider(".") %>%
    mutate(Tracecode = tracecode_i)

  if (nrow(operation_list_tmp) == 0) {
    print("no result!")
    next
  }

  operation_records_tbw <- operation_list_tmp %>%
    dplyr::select(trace_code = Tracecode,
                  operation_date = OperationDate,
                  operation_type = OperationType,
                  operation_conetent = Operation,
                  operation_memo = OperationMemo) %>%
    mutate(date = lubridate::as_date(operation_date, format = "%Y/%m/%d")) %>%
    filter(complete.cases(.)) %>%
    dplyr::select(-date)

  if (nrow(operation_records_tbw) == 0) {
    print("no writeable result!")
    next
  }

  operation_records_tbw %>%
    dbWriteTable(conn_taft, "resume_operation_detail_info", ., append = TRUE)
}
