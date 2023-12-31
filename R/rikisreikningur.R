library(dplyr)
library(tidyr)
library(readr)
library(readxl)
library(glue)
library(janitor)
library(here)
library(purrr)
library(stringr)
library(arrow)


download_rikisreikningur <- function() {
  financial_url <- "https://rkaup.blob.core.windows.net/datathon2023/rikisreikningur_fjarhagsgogn.csv"
  
  download.file(
    url = financial_url,
    destfile = here("data-raw", "rikisreikningur", "rikisreikningur_financial.csv")
  )
  staff_url <- "https://rkaup.blob.core.windows.net/datathon2023/rikisreikningur_mannaudsgogn.csv"
  download.file(
    url = staff_url,
    destfile = here("data-raw", "rikisreikningur", "rikisreikningur_staff.csv")
  )
}

process_rikisreikningur_financial <- function() {
  
  fin <- read_csv2(here("data-raw", "rikisreikningur", "rikisreikningur_financial.csv")) |> 
    select(
      raduneyti = Raduneyti_numer_og_heiti,
      stofnun = Stofnun_numer_og_heiti,
      timabil = Timabil,
      ar = Timabil_ar,
      heiti = Bokunartakn_heiti,
      fjarlagavidfang = Fjarlagavidfang_numer_og_heiti,
      malaflokkur = Malaflokkur_numer_og_heiti,
      yfirmalefnasvid = Yfirmalefnasvid,
      malefnasvid = Malefnasvid_numer_og_heiti,
      tegund = Tegund_numer_og_heiti,
      tegund1 = Tegund_L1_numer_og_heiti,
      tegund2 = Tegund_L2_numer_og_heiti,
      tegund3 = Tegund_L3_numer_og_heiti,
      threp1 = THREP_1,
      threp2 = THREP_2,
      threp3 = THREP_3,
      threp4 = THREP_4,
      threp5 = THREP_5,
      yfirflokkur = Rekstraryfirlit_yfirflokkur,
      undirflokkur = Rekstraryfirlit_undirflokkur,
      cofog = COFOG_numer_og_heiti,
      kr_stada_lok_ars = RAUNTOLUR_STADA_ARS,
      everything()
    ) |> 
    mutate_at(
      vars(raduneyti:cofog, -timabil, -ar),
      function(x) {
        str_replace(
          x,
          pattern = "^[0-9\\- \\.]+", 
          replacement = ""
        )
      }
    )
  
  write_parquet(
    fin,
    here("data", "rikisreikningur_financial.parquet")
  )
}

process_rikisreikningur_staff <- function() {
  staff <- read_csv2(here("data-raw", "rikisreikningur", "rikisreikningur_staff.csv")) |> 
    drop_na(stofnun_heiti) |> 
    mutate(
      ar = floor(dags_til_arman / 100),
      man = dags_til_arman %% 100,
      dags = clock::date_build(year = ar, month = man)
    ) |> 
    select(
      raduneyti = raduneyti_heiti,
      stofnun = stofnun_heiti,
      dags,
      everything()
    ) |> 
    filter(
      tegund == "Stöðugildi"
    ) |> 
    summarise(
      stodugildi = sum(stodugildi),
      fjoldi_kennitalna = sum(fjoldi_kennitalna),
      .by = c(raduneyti, stofnun, dags)
    )
  
  write_parquet(
    staff,
    here("data", "rikisreikningur_staff.parquet")
  )
}


upload_rikisreikningur_to_s3 <- function() {
  b <- s3_bucket("gagnathon")
  fin <- read_parquet(
    here("data", "rikisreikningur_financial.parquet")
  )
  
  write_dataset(
    dataset = fin, 
    path = b$path("rikisreikningur_financial.parquet"),
    format = "parquet"
  )
  
  staff <- read_parquet(
    here("data", "rikisreikningur_staff.parquet")
  )
  
  write_dataset(
    dataset = fin, 
    path = b$path("rikisreikningur_staff.parquet"),
    format = "parquet"
  )
}