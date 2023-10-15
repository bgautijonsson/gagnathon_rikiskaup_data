library(arrow)
library(glue)
library(clock)
library(readxl)
library(here)
library(janitor)
library(dplyr)
library(readr)
library(tidyr)
library(purrr)
library(stringr)

#' Fall til að sækja gögnin frá opnirreikningar.is og vista þau sem .parquet skrá
#'
#' Skilar engu, en vistar gögnin í möppunni data/
process_opnirreikningar <- function() {
  
  read_fun <- function(start, end, ...) {
    url <- glue("https://www.opnirreikningar.is/rest/csvExport?vendor_id=&type_id=&org_id=&timabil_fra={start}&timabil_til={end}")
    
    tmp <- tempfile()
    
    download.file(
      url = url,
      dest = tmp
    )
    # Svo að vefþjónninn loki ekki á mig vegna of mikillar umferðar
    Sys.sleep(0.1)
    
    # Nota try() því það getur komið fyrir að engin gögn séu í einhverjum mánuði
    # og vil ekki fá error
    out <- try(read_excel(tmp))
    
    if ("try-error" %in% class(out)) return(tibble())
    
    file_name <- str_c(
      start,
      "-",
      end,
      ".csv"
    )
    write_csv(
      out,
      here("data-raw", file_name)
    )
    
    return(out)
  }
  
  
  from <- date_build(2017, 1, 1)
  to <- Sys.Date()
  
  d <- tibble(
    start = seq.Date(from = from, to = to, by = "month")
  ) |> 
    mutate(
      # end verður þá síðasti dagur mánaðarins
      # og start er fyrstu dagur mánaðarins
      end = start - 1,
      end = lead(end)
    ) |> 
    drop_na() |> 
    mutate_at(
      vars(start, end), 
      format, "%d.%m.%Y"
    ) |> 
    mutate(
      data = map2(start, end, read_fun)
    ) 
  
  d <- d |> 
    select(data) |> 
    filter(
      map_dbl(data, nrow) > 0
    ) |> 
    unnest(data) |> 
    clean_names() |> 
    mutate(dags_greidslu = as_date(dags_greidslu)) |> 
    mutate_at(
      vars(kaupandi, birgi, tegund),
      as.factor
    )
  
  d |> 
    write_parquet(
      here("data", "opnirreikningar.parquet")
    )
}


process_opnirreikningar()
